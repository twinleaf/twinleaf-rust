//! Bounded retention of the newest samples of every live stream.
//!
//! [`Buffer`] keeps one [`Run`] per stream, and a run retains its rows as
//! frozen [`SampleBatch`]es: incoming batches are merged into chunks by a
//! [`BatchCoalescer`], and retention drops the oldest rows by slicing the
//! front chunk. Reads hand back slices of the retained chunks and copy at
//! most the still-accumulating rows they ask for.

use crate::data::{BatchCoalescer, Generations, SampleBatch};
use crate::tio::proto::identifiers::{ColumnId, ColumnKey, SampleNumber, StreamKey};
use crate::tio::proto::meta::{ColumnMetadata, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::Arc,
    time::Instant,
};

/// The newest samples of every stream that has delivered data, each capped at
/// `capacity` rows.
pub struct Buffer {
    capacity: usize,
    runs: HashMap<StreamKey, Run>,
}

impl Buffer {
    pub fn new(capacity: usize) -> Buffer {
        Buffer {
            capacity,
            runs: HashMap::new(),
        }
    }

    /// Retain `batch`'s rows, starting a new run when the parser stamped a new
    /// per-stream generation. Empty batches are ignored.
    pub fn process_batch(&mut self, batch: &SampleBatch) {
        if batch.is_empty() {
            return;
        }
        let stream_key = batch.stream_key();
        match self.runs.get_mut(&stream_key) {
            Some(run) if run.generations.stream == batch.generations().stream => run.append(batch),
            _ => {
                self.runs.insert(stream_key, Run::new(batch, self.capacity));
            }
        }
    }

    pub fn get_run(&self, stream_key: &StreamKey) -> Option<&Run> {
        self.runs.get(stream_key)
    }

    /// Iterate the streams currently holding a run, for callers sizing UI
    /// state (e.g. column widths) across all streams.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.runs.keys()
    }

    /// The stream's newest retained sample as a one-row batch: its metadata
    /// plus every column's last value, cheap enough to build once per render
    /// frame. `None` if the stream has no run or the run retains no rows.
    pub fn latest_row(&self, stream_key: &StreamKey) -> Option<SampleBatch> {
        self.runs.get(stream_key)?.last_row()
    }

    /// A column's metadata as of its stream's newest schema.
    pub fn column_metadata(&self, col: &ColumnKey) -> Option<Arc<ColumnMetadata>> {
        self.runs
            .get(&col.stream_key())?
            .column_metadata(col.column_id)
    }
}

/// One stream's samples since the parser last split its run, oldest row first.
///
/// Rows are appended into a [`BatchCoalescer`], which merges the small live
/// batches into chunks of about `capacity / 16` rows; the rows it still holds
/// are the run's tail, readable without completing them.
pub struct Run {
    /// The continuity generations stamped on every batch of this run.
    generations: Generations,
    stream: Arc<StreamMetadata>,
    segment: Arc<SegmentMetadata>,
    /// Column metadata in schema order, constant within a run.
    columns: Vec<Arc<ColumnMetadata>>,
    /// Completed chunks in row order; the coalescer holds the rows after them.
    chunks: VecDeque<SampleBatch>,
    coalescer: BatchCoalescer,
    /// Exclusive logical position of the next row appended to this run.
    next_row: u64,
    /// Retained rows, the tail included.
    rows: usize,
    capacity: usize,
    last_seen: Instant,
}

impl Run {
    fn new(batch: &SampleBatch, capacity: usize) -> Run {
        let mut run = Run {
            generations: batch.generations(),
            stream: batch.stream().clone(),
            segment: batch.segment().clone(),
            columns: batch
                .schema()
                .iter()
                .map(|column| column.metadata().clone())
                .collect(),
            chunks: VecDeque::new(),
            coalescer: BatchCoalescer::new(Some((capacity / 16).clamp(256, 65_536))),
            next_row: 0,
            rows: 0,
            capacity,
            last_seen: Instant::now(),
        };
        run.append(batch);
        run
    }

    fn append(&mut self, batch: &SampleBatch) {
        debug_assert_eq!(
            batch.schema().len(),
            self.columns.len(),
            "the parser guarantees a constant schema within a run"
        );
        self.coalescer.push_batch(batch);
        self.take_completed_chunks();

        self.stream = batch.stream().clone();
        self.segment = batch.segment().clone();
        self.next_row += batch.len() as u64;
        self.rows += batch.len();
        self.last_seen = Instant::now();
        self.evict();
    }

    fn take_completed_chunks(&mut self) {
        while let Some(chunk) = self.coalescer.next_completed_batch() {
            self.chunks.push_back(chunk);
        }
    }

    /// Drop the oldest rows down to `capacity`, slicing the front chunk so
    /// retention stays row-exact.
    fn evict(&mut self) {
        let Some(mut excess) = self.rows.checked_sub(self.capacity) else {
            return;
        };
        if self.rows - self.coalescer.buffered_len() < excess {
            // The rows to drop reach into the tail, so freeze it first.
            self.coalescer.finish_buffered_batch();
            self.take_completed_chunks();
        }
        while excess > 0 {
            let front = self.chunks.front_mut().expect("chunks cover the excess");
            let dropped = excess.min(front.len());
            if dropped == front.len() {
                self.chunks.pop_front();
            } else {
                *front = front.slice(dropped..front.len());
            }
            self.rows -= dropped;
            excess -= dropped;
        }
    }

    /// The retained rows within `rows`, as contiguous spans in row order.
    pub(super) fn row_spans(&self, rows: Range<u64>) -> Vec<SampleBatch> {
        let retained = self.retained_rows();
        let (first, last) = (rows.start.max(retained.start), rows.end.min(retained.end));
        if first >= last {
            return Vec::new();
        }
        let start = (first - retained.start) as usize;
        let end = (last - retained.start) as usize;

        let mut spans = Vec::new();
        let mut offset = 0;
        for batch in &self.chunks {
            let from = start.max(offset);
            let to = end.min(offset + batch.len());
            if from < to {
                spans.push(batch.slice(from - offset..to - offset));
            }
            offset += batch.len();
        }
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            let (from, to) = (start.max(offset), end.min(offset + buffered));
            if from < to {
                spans.push(
                    self.coalescer
                        .buffered_rows(from - offset..to - offset)
                        .expect("a nonempty buffered tail"),
                );
            }
        }
        spans
    }

    /// The newest retained row, as a one-row batch.
    fn last_row(&self) -> Option<SampleBatch> {
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            return self.coalescer.buffered_rows(buffered - 1..buffered);
        }
        let chunk = self.chunks.back()?;
        Some(chunk.slice(chunk.len() - 1..chunk.len()))
    }

    fn column_metadata(&self, column_id: ColumnId) -> Option<Arc<ColumnMetadata>> {
        self.columns
            .iter()
            .find(|column| column.index == column_id)
            .cloned()
    }

    pub fn generations(&self) -> Generations {
        self.generations
    }

    /// Wall-clock time this run last received a batch, refreshed on every
    /// [`Buffer::process_batch`] update. Drives staleness display for callers
    /// like the monitor TUI.
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }

    pub fn stream(&self) -> &Arc<StreamMetadata> {
        &self.stream
    }

    /// The run's newest segment metadata (sampling rate, decimation, ...),
    /// e.g. for callers that need the raw ints rather than
    /// [`Self::effective_rate`].
    pub fn segment(&self) -> &Arc<SegmentMetadata> {
        &self.segment
    }

    /// Samples per second after decimation.
    pub fn effective_rate(&self) -> f64 {
        self.segment.sampling_rate as f64 / self.segment.decimation as f64
    }

    /// End-of-sample time of the newest retained row.
    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_row().map(|row| row.timestamps()[0])
    }

    pub fn last_sample_number(&self) -> Option<SampleNumber> {
        self.last_row().map(|row| row.sample_numbers()[0])
    }

    /// The logical positions of the rows still retained, counted from the
    /// run's first row.
    pub fn retained_rows(&self) -> Range<u64> {
        self.next_row - self.rows as u64..self.next_row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::sample::{BatchContext, SampleBatchBuilder};
    use crate::data::{ColumnArray, ColumnData, ColumnOp, ColumnProcessor};
    use crate::tio::proto::meta::{DeviceMetadata, MetadataEpoch, MetadataFilter};
    use crate::tio::proto::{DataType, DeviceRoute};

    /// Records every sample it is fed, plus the length of each span it was fed as,
    /// so tests can see where the buffer's chunk boundaries fall.
    #[derive(Default)]
    struct Collect {
        samples: Vec<(f64, f64)>,
        spans: Vec<usize>,
        resets: usize,
    }

    impl ColumnOp for Collect {
        type Output = Vec<(f64, f64)>;

        fn reset(&mut self) {
            self.samples.clear();
            self.spans.clear();
            self.resets += 1;
        }

        fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
            assert_eq!(
                timestamps.len(),
                values.len(),
                "spans must line up row-wise"
            );
            self.spans.push(timestamps.len());
            for (row, &t) in timestamps.iter().enumerate() {
                let value = values.get(row).try_as_f64().expect("a numeric column");
                self.samples.push((t, value));
            }
        }

        fn output(&self) -> &Self::Output {
            &self.samples
        }
    }

    struct Fixture {
        stream_key: StreamKey,
        columns: Vec<Arc<ColumnMetadata>>,
        column_keys: Vec<ColumnKey>,
        device: Arc<DeviceMetadata>,
        stream: Arc<StreamMetadata>,
        segment: Arc<SegmentMetadata>,
    }

    impl Fixture {
        /// Push one batch stamped with `stream_generation`; a change of generation is what
        /// starts a new run.
        fn push_rows(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            rows: &[(SampleNumber, Vec<ColumnData>)],
        ) {
            self.push_in_segment(buffer, stream_generation, &self.segment, rows);
        }

        fn push_in_segment(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            segment: &Arc<SegmentMetadata>,
            rows: &[(SampleNumber, Vec<ColumnData>)],
        ) {
            let mut builder = SampleBatchBuilder::new(
                BatchContext::new(
                    self.stream_key,
                    None,
                    Generations {
                        stream: stream_generation,
                        device: 0,
                        global: 0,
                    },
                    segment.clone(),
                    self.stream.clone(),
                    self.device.clone(),
                ),
                self.columns
                    .iter()
                    .map(|metadata| (metadata.clone(), metadata.data_type.buffer_type())),
                rows.len(),
            );
            for &(sample_number, ref row) in rows {
                assert_eq!(row.len(), self.columns.len());
                builder.push_row(sample_number, row.iter().cloned());
            }
            let batch = builder.finish();
            buffer.process_batch(&batch);
        }

        /// Push `samples` as one batch of a single float column whose values are
        /// the sample numbers.
        fn push_floats(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            samples: Range<SampleNumber>,
        ) {
            let rows: Vec<_> = samples
                .map(|n| (n, vec![ColumnData::Float(f64::from(n))]))
                .collect();
            self.push_rows(buffer, stream_generation, &rows);
        }
    }

    /// One stream sampled at 1 Hz from time zero, so a row's timestamp is its
    /// sample number plus one.
    fn test_fixture(column_types: &[DataType]) -> Fixture {
        let route = DeviceRoute::root();
        let stream_id = 1;

        let columns: Vec<_> = column_types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                Arc::new(ColumnMetadata {
                    stream_id,
                    index,
                    data_type: *data_type,
                    name: format!("col_{index}"),
                    units: format!("u{index}"),
                    description: format!("column {index}"),
                })
            })
            .collect();

        Fixture {
            stream_key: StreamKey::new(route, stream_id),
            column_keys: columns
                .iter()
                .map(|metadata| ColumnKey::new(route, stream_id, metadata.index))
                .collect(),
            device: Arc::new(DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            }),
            stream: Arc::new(StreamMetadata {
                stream_id,
                name: "test-stream".to_string(),
                n_columns: columns.len(),
                n_segments: 1,
                sample_size: 0,
                buf_samples: 1024,
            }),
            segment: Arc::new(SegmentMetadata {
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
            }),
            columns,
        }
    }

    /// The samples a float run should hold for sample numbers `samples`.
    fn expected(samples: Range<SampleNumber>) -> Vec<(f64, f64)> {
        samples
            .map(|n| (f64::from(n) + 1.0, f64::from(n)))
            .collect()
    }

    #[test]
    fn a_run_retains_every_row_until_capacity_is_reached() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::Float64]);

        for start in (0..300).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.get_run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..300);
        assert_eq!(run.last_timestamp(), Some(300.0));
        assert_eq!(run.last_sample_number(), Some(299));
        assert_eq!(run.effective_rate(), 1.0);

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..300));
    }

    #[test]
    fn reads_span_the_frozen_chunks_and_the_unfrozen_tail() {
        let mut buffer = Buffer::new(4096);
        let fx = test_fixture(&[DataType::Float64]);

        // The chunk target is capacity / 16, so every third batch of 100 rows
        // completes a chunk and the rest stay in the tail.
        for start in (0..650).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..(start + 100).min(650));
        }

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..650));
        assert_eq!(
            processor.op().spans,
            vec![300, 300, 50],
            "two frozen chunks then the buffered tail"
        );
        assert_eq!(processor.op().resets, 1);

        // Rows appended after the read continue the same run, tail included.
        fx.push_floats(&mut buffer, 1, 650..660);
        assert_eq!(processor.catch_up(&buffer), &expected(0..660));
        assert_eq!(
            processor.op().resets,
            1,
            "an append must not force a replay"
        );
    }

    #[test]
    fn eviction_is_row_exact_across_chunk_boundaries() {
        let mut buffer = Buffer::new(1000);
        let fx = test_fixture(&[DataType::Float64]);

        for start in (0..3000).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.get_run(&fx.stream_key).expect("the run");
        assert_eq!(
            run.retained_rows(),
            2000..3000,
            "retention drops whole rows, not whole chunks"
        );
        assert_eq!(run.last_timestamp(), Some(3000.0));

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(2000..3000));
        assert!(
            processor.op().spans.len() > 1,
            "1000 rows must span several chunks, got {:?}",
            processor.op().spans
        );
        assert_eq!(processor.op().spans.iter().sum::<usize>(), 1000);
    }

    #[test]
    fn a_segment_rollover_keeps_the_run_and_delivers_both_sides() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::Float64]);
        fx.push_floats(&mut buffer, 1, 0..4);

        // A seamless rollover keeps the stream generation, so the run continues even
        // though the new segment starts a new chunk.
        let mut rolled = (*fx.segment).clone();
        rolled.segment_id = 1;
        rolled.start_time = 4;
        let rolled = Arc::new(rolled);
        let rows: Vec<_> = (0..3)
            .map(|n| (n, vec![ColumnData::Float(f64::from(n) + 100.0)]))
            .collect();
        fx.push_in_segment(&mut buffer, 1, &rolled, &rows);

        let run = buffer.get_run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..7);
        assert!(Arc::ptr_eq(run.segment(), &rolled), "the newest segment");

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(
            out,
            vec![
                (1.0, 0.0),
                (2.0, 1.0),
                (3.0, 2.0),
                (4.0, 3.0),
                (5.0, 100.0),
                (6.0, 101.0),
                (7.0, 102.0),
            ]
        );
        assert_eq!(processor.op().spans, vec![4, 3], "one span per segment");
        assert_eq!(processor.op().resets, 1);
    }

    #[test]
    fn a_new_stream_generation_starts_a_run_that_discards_the_old_rows() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::Float64]);
        fx.push_floats(&mut buffer, 1, 0..500);
        assert_eq!(
            buffer.get_run(&fx.stream_key).unwrap().retained_rows(),
            0..500
        );

        fx.push_floats(&mut buffer, 2, 0..3);
        let run = buffer.get_run(&fx.stream_key).expect("the new run");
        assert_eq!(run.generations().stream, 2);
        assert_eq!(
            run.retained_rows(),
            0..3,
            "row positions restart with the run"
        );
        assert_eq!(run.last_sample_number(), Some(2));

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..3));
    }

    #[test]
    fn latest_row_is_the_newest_retained_row_with_its_typed_values() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::Float64, DataType::Int64, DataType::UInt64]);

        fx.push_rows(
            &mut buffer,
            1,
            &[
                (
                    0,
                    vec![
                        ColumnData::Float(0.5),
                        ColumnData::Int(-1),
                        ColumnData::UInt(10),
                    ],
                ),
                (
                    1,
                    vec![
                        ColumnData::Float(1.5),
                        ColumnData::Int(-2),
                        ColumnData::UInt(11),
                    ],
                ),
            ],
        );
        // A later, separate batch must overwrite the newest row.
        fx.push_rows(
            &mut buffer,
            1,
            &[(
                2,
                vec![
                    ColumnData::Float(2.5),
                    ColumnData::Int(-3),
                    ColumnData::UInt(12),
                ],
            )],
        );

        let row = buffer.latest_row(&fx.stream_key).expect("the newest row");
        assert_eq!(row.len(), 1);
        assert_eq!(row.sample_numbers(), [2]);
        assert_eq!(row.timestamps(), [3.0]);
        assert!(Arc::ptr_eq(row.stream(), &fx.stream));
        assert!(Arc::ptr_eq(row.segment(), &fx.segment));

        // Values come back in schema order, each in its column's own variant.
        let values: Vec<ColumnData> = row.row(0).expect("the only row").values().collect();
        assert_eq!(values.len(), 3);
        assert!(matches!(values[0], ColumnData::Float(v) if v == 2.5));
        assert!(matches!(values[1], ColumnData::Int(-3)));
        assert!(matches!(values[2], ColumnData::UInt(12)));

        // No run for a stream that never received data.
        let other = StreamKey::new(DeviceRoute::root(), 99);
        assert!(buffer.latest_row(&other).is_none());
        assert!(buffer.get_run(&other).is_none());
    }

    #[test]
    fn latest_row_follows_the_newest_row_out_of_the_tail_and_across_eviction() {
        let mut buffer = Buffer::new(64);
        let fx = test_fixture(&[DataType::Float64]);

        fx.push_floats(&mut buffer, 1, 0..1);
        let row = buffer.latest_row(&fx.stream_key).expect("the buffered row");
        assert_eq!(row.sample_numbers(), [0]);

        fx.push_floats(&mut buffer, 1, 1..200);
        let row = buffer.latest_row(&fx.stream_key).expect("the newest row");
        assert_eq!(row.sample_numbers(), [199]);
        assert_eq!(row.timestamps(), [200.0]);
        assert_eq!(
            buffer.get_run(&fx.stream_key).unwrap().retained_rows(),
            136..200
        );
    }

    #[test]
    fn column_metadata_comes_from_the_newest_schema() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::Float64]);
        assert!(buffer.column_metadata(&fx.column_keys[0]).is_none());

        fx.push_floats(&mut buffer, 1, 0..2);
        let metadata = buffer
            .column_metadata(&fx.column_keys[0])
            .expect("the column");
        assert_eq!(metadata.description, "column 0");
        assert_eq!(metadata.units, "u0");

        // A schema change rides a new stream generation, and the new run's columns
        // replace the old ones.
        let changed = test_fixture(&[DataType::Float64, DataType::UInt64]);
        changed.push_rows(
            &mut buffer,
            2,
            &[(0, vec![ColumnData::Float(1.0), ColumnData::UInt(2)])],
        );
        assert_eq!(
            buffer
                .column_metadata(&changed.column_keys[1])
                .expect("the added column")
                .data_type,
            DataType::UInt64
        );
        assert!(buffer
            .column_metadata(&ColumnKey::new(DeviceRoute::root(), 1, 7))
            .is_none());
    }

    #[test]
    fn stream_keys_lists_every_stream_that_delivered_data() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::Float64]);
        fx.push_floats(&mut buffer, 1, 0..2);

        let keys: Vec<_> = buffer.stream_keys().copied().collect();
        assert_eq!(keys, vec![fx.stream_key]);
    }

    #[test]
    fn an_empty_batch_never_starts_a_run() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::Float64]);
        fx.push_floats(&mut buffer, 1, 0..0);
        assert!(buffer.get_run(&fx.stream_key).is_none());
        assert_eq!(buffer.stream_keys().count(), 0);
    }
}

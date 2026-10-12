//! Bounded retention of the newest samples of every live stream.
//!
//! [`Buffer`] keeps one [`Run`] per stream. A run retains its rows as frozen
//! [`SampleBatch`]es. A [`BatchCoalescer`] merges incoming batches into
//! chunks, and retention drops the oldest rows by slicing the front chunk.
//! Reads return slices of the retained chunks and copy only the
//! still-accumulating rows they ask for.

use super::coalesce::BatchCoalescer;
use super::metadata::{ColumnKey, ColumnRecord, SegmentRecord, StreamKey, StreamRecord};
use super::sample::{Generations, SampleBatch};
use crate::proto::data as wire;
use crate::proto::{ColumnId, SampleNumber};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::time::Instant;

/// The newest samples of every stream that has delivered data, each capped at
/// `capacity` rows.
///
/// # Examples
///
/// This example replays a recorded log of three samples into a buffer that
/// keeps two.
///
/// ```
/// use twinleaf::data::{Buffer, LogFile};
/// # use std::io::Write;
/// # use twinleaf::proto::data::{self as wire, DataType};
/// # use twinleaf::proto::sync::Epoch;
/// # use twinleaf::tio::Packet;
/// # use twinleaf::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, route) = (StreamId::new(1), DeviceRoute::root());
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1" }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::default(), epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     file.write_all(Packet::metadata(record, wire::MetadataFlags::UPDATE, route)?.as_bytes())?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # file.write_all(Packet::samples(1, 0, 0, &data, route)?.as_bytes())?;
/// # let path = file.path();
/// let mut buffer = Buffer::new(2);
/// for batch in LogFile::open(path)?.scan(false).batches(4096) {
///     buffer.process_batch(&batch?);
/// }
///
/// let key = *buffer.stream_keys().next().expect("one stream");
/// let run = buffer.run(&key).expect("a run");
/// assert_eq!(run.retained_rows(), 1..3);
/// assert_eq!(run.effective_rate(), 10.0);
///
/// let latest = buffer.latest_row(&key).expect("a retained row");
/// println!("{}", latest.row(0).expect("one row"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Buffer {
    capacity: usize,
    runs: HashMap<StreamKey, Run>,
}

impl Buffer {
    /// Creates a buffer that keeps at most `capacity` rows of each stream.
    pub fn new(capacity: usize) -> Buffer {
        Buffer {
            capacity,
            runs: HashMap::new(),
        }
    }

    /// Appends `batch`'s rows to its stream's run. A batch with a new
    /// per-stream generation starts a new run. Empty batches are ignored.
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

    /// Returns the stream's current run, or `None` if it has delivered no data.
    pub fn run(&self, stream_key: &StreamKey) -> Option<&Run> {
        self.runs.get(stream_key)
    }

    /// Returns an iterator over the streams that hold a run, in no particular
    /// order.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.runs.keys()
    }

    /// Returns the stream's newest retained row as a one-row batch, copying at
    /// most that row. Returns `None` if the stream has no run or the run
    /// retains no rows.
    pub fn latest_row(&self, stream_key: &StreamKey) -> Option<SampleBatch> {
        self.runs.get(stream_key)?.last_row()
    }

    /// Returns a column's metadata as of its stream's newest batch, or `None`
    /// if the stream has no run or no such column.
    pub fn column_metadata(&self, col: &ColumnKey) -> Option<wire::Column<'_>> {
        self.runs
            .get(&col.stream_key())?
            .column_metadata(col.column_id)
    }
}

/// The retained rows of one stream's current run, oldest first.
pub struct Run {
    /// The continuity generations stamped on every batch of this run.
    generations: Generations,
    stream: StreamRecord,
    segment: SegmentRecord,
    /// Column metadata in schema order, constant within a run.
    columns: Vec<ColumnRecord>,
    /// Completed chunks in row order. The coalescer holds the rows after them.
    chunks: VecDeque<SampleBatch>,
    coalescer: BatchCoalescer,
    /// Exclusive logical position of the next row appended to this run.
    next_row: u64,
    /// Retained rows, the tail included.
    rows: usize,
    capacity: usize,
    last_seen: Instant,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run")
            .field("generations", &self.generations)
            .field("rows", &self.rows)
            .field("capacity", &self.capacity)
            .field("next_row", &self.next_row)
            .field("last_seen", &self.last_seen)
            .finish_non_exhaustive()
    }
}

impl Run {
    fn new(batch: &SampleBatch, capacity: usize) -> Run {
        let (_, stream, segment) = batch.records();
        let mut run = Run {
            generations: batch.generations(),
            stream: stream.clone(),
            segment: segment.clone(),
            columns: batch
                .schema()
                .iter()
                .map(|column| column.record().clone())
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

        let (_, stream, segment) = batch.records();
        self.stream = stream.clone();
        self.segment = segment.clone();
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

    /// Drops the oldest rows until at most `capacity` remain. A cut that falls
    /// inside the front chunk slices it.
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

    /// Returns the retained rows within `rows` as contiguous spans in row
    /// order.
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

    /// Returns the newest retained row as a one-row batch.
    fn last_row(&self) -> Option<SampleBatch> {
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            return self.coalescer.buffered_rows(buffered - 1..buffered);
        }
        let chunk = self.chunks.back()?;
        Some(chunk.slice(chunk.len() - 1..chunk.len()))
    }

    fn column_metadata(&self, column_id: ColumnId) -> Option<wire::Column<'_>> {
        self.columns
            .iter()
            .find(|column| column.get().index == column_id)
            .map(ColumnRecord::get)
    }

    /// Returns the continuity generations stamped on every batch of this run.
    pub fn generations(&self) -> Generations {
        self.generations
    }

    /// Returns when this run last received a batch through
    /// [`Buffer::process_batch`].
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }

    /// Returns the run's stream descriptor as of its newest batch.
    pub fn stream(&self) -> wire::Stream<'_> {
        self.stream.get()
    }

    /// Returns the run's segment descriptor as of its newest batch. It holds
    /// the raw sampling rate and decimation behind [`Self::effective_rate`].
    pub fn segment(&self) -> wire::Segment<'_> {
        self.segment.get()
    }

    /// Returns the sample rate in hertz after decimation, or zero for a
    /// segment whose rate or decimation is zero. The parser emits no rows from
    /// such a segment.
    pub fn effective_rate(&self) -> f64 {
        self.segment.effective_rate().unwrap_or(0.0)
    }

    /// Returns the end-of-sample time of the newest retained row, in seconds
    /// after the segment's epoch, or `None` if no rows remain.
    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_row().map(|row| row.timestamps()[0])
    }

    /// Returns the sample number of the newest retained row, or `None` if no
    /// rows remain.
    pub fn last_sample_number(&self) -> Option<SampleNumber> {
        self.last_row().map(|row| row.sample_numbers()[0])
    }

    /// Returns the logical positions of the retained rows, counted from the
    /// run's first row.
    pub fn retained_rows(&self) -> Range<u64> {
        self.next_row - self.rows as u64..self.next_row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::metadata::{buffer_type, DeviceRecord};
    use crate::data::pipeline::{ColumnOp, ColumnProcessor};
    use crate::data::sample::{BatchContext, ColumnArray, ColumnData, SampleBatchBuilder};
    use crate::proto::data::DataType;
    use crate::proto::DeviceRoute;

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
        columns: Vec<ColumnRecord>,
        column_keys: Vec<ColumnKey>,
        device: DeviceRecord,
        stream: StreamRecord,
        segment: SegmentRecord,
    }

    impl Fixture {
        /// Push one batch stamped with `stream_generation`; a change of generation is what
        /// starts a new run.
        fn push_rows(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            rows: &[(u32, Vec<ColumnData>)],
        ) {
            self.push_in_segment(buffer, stream_generation, &self.segment, rows);
        }

        fn push_in_segment(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            segment: &SegmentRecord,
            rows: &[(u32, Vec<ColumnData>)],
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
                    .map(|metadata| (metadata.clone(), buffer_type(metadata.get().data_type))),
                rows.len(),
            );
            for &(sample_number, ref row) in rows {
                assert_eq!(row.len(), self.columns.len());
                builder.push_row(SampleNumber::new(sample_number), row.iter().cloned());
            }
            let batch = builder.finish();
            buffer.process_batch(&batch);
        }

        /// Push `samples` as one batch of a single float column whose values are
        /// the sample numbers.
        fn push_floats(&self, buffer: &mut Buffer, stream_generation: u32, samples: Range<u32>) {
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

        let columns: Vec<ColumnRecord> = column_types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                let index = index as u8;
                ColumnRecord::encode(wire::Column {
                    name: &format!("col_{index}"),
                    units: &format!("u{index}"),
                    description: &format!("column {index}"),
                    ..fixtures::column(stream_id, index, *data_type)
                })
                .unwrap()
            })
            .collect();

        Fixture {
            stream_key: StreamKey::new(route, crate::proto::StreamId::new(stream_id)),
            column_keys: (0..columns.len())
                .map(|index| {
                    ColumnKey::new(
                        route,
                        crate::proto::StreamId::new(stream_id),
                        ColumnId::new(u8::try_from(index).unwrap()),
                    )
                })
                .collect(),
            device: DeviceRecord::encode(fixtures::device()).unwrap(),
            stream: StreamRecord::encode(wire::Stream {
                n_columns: columns.len() as u8,
                sample_size: 0,
                buf_samples: 1024,
                ..fixtures::stream(stream_id)
            })
            .unwrap(),
            segment: SegmentRecord::encode(fixtures::segment(stream_id)).unwrap(),
            columns,
        }
    }

    /// The samples a float run should hold for sample numbers `samples`.
    fn expected(samples: Range<u32>) -> Vec<(f64, f64)> {
        samples
            .map(|n| (f64::from(n) + 1.0, f64::from(n)))
            .collect()
    }

    #[test]
    fn a_run_retains_every_row_until_capacity_is_reached() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);

        for start in (0..300).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..300);
        assert_eq!(run.last_timestamp(), Some(300.0));
        assert_eq!(run.last_sample_number(), Some(SampleNumber::new(299)));
        assert_eq!(run.effective_rate(), 1.0);

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..300));
    }

    #[test]
    fn reads_span_the_frozen_chunks_and_the_unfrozen_tail() {
        let mut buffer = Buffer::new(4096);
        let fx = test_fixture(&[DataType::F64]);

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
        let fx = test_fixture(&[DataType::F64]);

        for start in (0..3000).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.run(&fx.stream_key).expect("the run");
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
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..4);

        // A seamless rollover keeps the stream generation, so the run continues even
        // though the new segment starts a new chunk.
        let rolled = SegmentRecord::encode(wire::Segment {
            segment_id: crate::proto::SegmentId::new(1),
            start_time: 4,
            ..fx.segment.get()
        })
        .unwrap();
        let rows: Vec<_> = (0..3)
            .map(|n| (n, vec![ColumnData::Float(f64::from(n) + 100.0)]))
            .collect();
        fx.push_in_segment(&mut buffer, 1, &rolled, &rows);

        let run = buffer.run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..7);
        assert_eq!(run.segment(), rolled.get(), "the newest segment");

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
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..500);
        assert_eq!(buffer.run(&fx.stream_key).unwrap().retained_rows(), 0..500);

        fx.push_floats(&mut buffer, 2, 0..3);
        let run = buffer.run(&fx.stream_key).expect("the new run");
        assert_eq!(run.generations().stream, 2);
        assert_eq!(
            run.retained_rows(),
            0..3,
            "row positions restart with the run"
        );
        assert_eq!(run.last_sample_number(), Some(SampleNumber::new(2)));

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..3));
    }

    #[test]
    fn latest_row_is_the_newest_retained_row_with_its_typed_values() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64, DataType::I64, DataType::U64]);

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
        assert_eq!(row.stream(), fx.stream.get());
        assert_eq!(row.segment(), fx.segment.get());

        // Values come back in schema order, each in its column's own variant.
        let values: Vec<ColumnData> = row.row(0).expect("the only row").values().collect();
        assert_eq!(values.len(), 3);
        assert!(matches!(values[0], ColumnData::Float(v) if v == 2.5));
        assert!(matches!(values[1], ColumnData::Int(-3)));
        assert!(matches!(values[2], ColumnData::UInt(12)));

        // No run for a stream that never received data.
        let other = StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(99));
        assert!(buffer.latest_row(&other).is_none());
        assert!(buffer.run(&other).is_none());
    }

    #[test]
    fn latest_row_follows_the_newest_row_out_of_the_tail_and_across_eviction() {
        let mut buffer = Buffer::new(64);
        let fx = test_fixture(&[DataType::F64]);

        fx.push_floats(&mut buffer, 1, 0..1);
        let row = buffer.latest_row(&fx.stream_key).expect("the buffered row");
        assert_eq!(row.sample_numbers(), [0]);

        fx.push_floats(&mut buffer, 1, 1..200);
        let row = buffer.latest_row(&fx.stream_key).expect("the newest row");
        assert_eq!(row.sample_numbers(), [199]);
        assert_eq!(row.timestamps(), [200.0]);
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            136..200
        );
    }

    #[test]
    fn column_metadata_comes_from_the_newest_schema() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        assert!(buffer.column_metadata(&fx.column_keys[0]).is_none());

        fx.push_floats(&mut buffer, 1, 0..2);
        let metadata = buffer
            .column_metadata(&fx.column_keys[0])
            .expect("the column");
        assert_eq!(metadata.description, "column 0");
        assert_eq!(metadata.units, "u0");

        // A schema change rides a new stream generation, and the new run's columns
        // replace the old ones.
        let changed = test_fixture(&[DataType::F64, DataType::U64]);
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
            DataType::U64
        );
        assert!(buffer
            .column_metadata(&ColumnKey::new(
                DeviceRoute::root(),
                crate::proto::StreamId::new(1),
                ColumnId::new(7),
            ))
            .is_none());
    }

    #[test]
    fn stream_keys_lists_every_stream_that_delivered_data() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..2);

        let keys: Vec<_> = buffer.stream_keys().copied().collect();
        assert_eq!(keys, vec![fx.stream_key]);
    }

    #[test]
    fn an_empty_batch_never_starts_a_run() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..0);
        assert!(buffer.run(&fx.stream_key).is_none());
        assert_eq!(buffer.stream_keys().count(), 0);
    }
}

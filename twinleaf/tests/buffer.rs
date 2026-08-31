use std::ops::Range;
use std::sync::Arc;

use twinleaf::data::{
    Buffer, ColumnArray, ColumnBuilder, ColumnData, ColumnOp, ColumnProcessor, Generations,
    SampleBatch, Series,
};
use twinleaf::tio::proto::identifiers::{ColumnKey, SampleNumber, StreamKey};
use twinleaf::tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataEpoch, MetadataFilter, SegmentMetadata, StreamMetadata,
};
use twinleaf::tio::proto::{DataType, DeviceRoute};

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
        let sample_numbers: Vec<SampleNumber> = rows.iter().map(|(n, _)| *n).collect();
        let series = self
            .columns
            .iter()
            .enumerate()
            .map(|(ci, metadata)| {
                let mut values = ColumnBuilder::empty_for(metadata.data_type.buffer_type());
                for (_, row) in rows {
                    assert_eq!(row.len(), self.columns.len());
                    values.push_data(&row[ci]);
                }
                Series::new(metadata.index, metadata.clone(), values)
            })
            .collect();

        let batch = SampleBatch::new(
            DeviceRoute::root(),
            None,
            Generations {
                stream: stream_generation,
                device: 0,
                global: 0,
            },
            sample_numbers,
            series,
            segment.clone(),
            self.stream.clone(),
            self.device.clone(),
        );
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

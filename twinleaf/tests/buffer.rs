use std::collections::HashMap;
use std::sync::Arc;

use twinleaf::data::{Buffer, Column, ColumnBatch, ColumnData, CursorPosition, ReadError, Sample};
use twinleaf::tio::proto::identifiers::{ColumnKey, SampleNumber, StreamKey};
use twinleaf::tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataEpoch, MetadataFilter, SegmentMetadata, StreamMetadata,
};
use twinleaf::tio::proto::{DataType, DeviceRoute, StreamDataPayload};

fn test_fixture(
    column_types: &[DataType],
) -> (
    StreamKey,
    Vec<Arc<ColumnMetadata>>,
    Vec<ColumnKey>,
    Arc<DeviceMetadata>,
    Arc<StreamMetadata>,
    Arc<SegmentMetadata>,
) {
    let route = DeviceRoute::root();
    let stream_id = 1;
    let stream_key = StreamKey::new(route.clone(), stream_id);

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
        n_columns: column_types.len(),
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

    let columns: Vec<_> = column_types
        .iter()
        .enumerate()
        .map(|(index, data_type)| {
            Arc::new(ColumnMetadata {
                stream_id,
                index,
                data_type: *data_type,
                name: format!("col_{index}"),
                units: String::new(),
                description: String::new(),
            })
        })
        .collect();

    let column_keys = columns
        .iter()
        .map(|metadata| ColumnKey::new(route.clone(), stream_id, metadata.index))
        .collect();

    (stream_key, columns, column_keys, device, stream, segment)
}

fn push_rows(
    buffer: &mut Buffer,
    stream_key: &StreamKey,
    columns: &[Arc<ColumnMetadata>],
    device: &Arc<DeviceMetadata>,
    stream: &Arc<StreamMetadata>,
    segment: &Arc<SegmentMetadata>,
    rows: &[Vec<ColumnData>],
) {
    for (sample_idx, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), columns.len());
        let sample = Sample {
            n: sample_idx as SampleNumber,
            columns: columns
                .iter()
                .zip(row.iter())
                .map(|(desc, value)| Column {
                    value: value.clone(),
                    desc: desc.clone(),
                })
                .collect(),
            segment: segment.clone(),
            stream: stream.clone(),
            device: device.clone(),
            source: StreamDataPayload {
                stream_id: stream.stream_id,
                first_sample_n: sample_idx as SampleNumber,
                segment_id: segment.segment_id,
                data: Vec::new(),
            },
            boundary: None,
        };
        buffer.process_sample(sample, stream_key.clone());
    }
}

fn push_rows_with_sample_numbers(
    buffer: &mut Buffer,
    stream_key: &StreamKey,
    columns: &[Arc<ColumnMetadata>],
    device: &Arc<DeviceMetadata>,
    stream: &Arc<StreamMetadata>,
    segment: &Arc<SegmentMetadata>,
    rows: &[(SampleNumber, Vec<ColumnData>)],
) {
    for (sample_n, row) in rows.iter() {
        assert_eq!(row.len(), columns.len());
        let sample = Sample {
            n: *sample_n,
            columns: columns
                .iter()
                .zip(row.iter())
                .map(|(desc, value)| Column {
                    value: value.clone(),
                    desc: desc.clone(),
                })
                .collect(),
            segment: segment.clone(),
            stream: stream.clone(),
            device: device.clone(),
            source: StreamDataPayload {
                stream_id: stream.stream_id,
                first_sample_n: *sample_n,
                segment_id: segment.segment_id,
                data: Vec::new(),
            },
            boundary: None,
        };
        buffer.process_sample(sample, stream_key.clone());
    }
}

#[test]
fn read_aligned_time_range_in_range() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(0.0)],
            vec![ColumnData::Float(1.0)],
            vec![ColumnData::Float(2.0)],
            vec![ColumnData::Float(3.0)],
            vec![ColumnData::Float(4.0)],
            vec![ColumnData::Float(5.0)],
        ],
    );

    let window = buffer
        .read_aligned_time_range(&column_keys, 2.0, 4.0)
        .unwrap();

    assert_eq!(window.timestamps, vec![2.0, 3.0, 4.0]);
    assert_eq!(window.sample_numbers[&stream_key], vec![1, 2, 3]);

    let batch = &window.columns[&column_keys[0]];
    match batch {
        ColumnBatch::F64(values) => assert_eq!(values, &vec![1.0, 2.0, 3.0]),
        _ => panic!("expected f64 column batch"),
    }

    assert_eq!(
        window.timestamps.len(),
        window.sample_numbers[&stream_key].len()
    );
    assert_eq!(window.timestamps.len(), batch.len());
}

#[test]
fn read_aligned_time_range_normalizes_reversed_bounds() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(0.0)],
            vec![ColumnData::Float(1.0)],
            vec![ColumnData::Float(2.0)],
            vec![ColumnData::Float(3.0)],
            vec![ColumnData::Float(4.0)],
            vec![ColumnData::Float(5.0)],
        ],
    );

    let window = buffer
        .read_aligned_time_range(&column_keys, 4.0, 2.0)
        .unwrap();

    assert_eq!(window.timestamps, vec![2.0, 3.0, 4.0]);
    assert_eq!(window.sample_numbers[&stream_key], vec![1, 2, 3]);
}

#[test]
fn read_aligned_time_range_includes_exact_boundaries() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(10.0)],
            vec![ColumnData::Float(11.0)],
            vec![ColumnData::Float(12.0)],
            vec![ColumnData::Float(13.0)],
        ],
    );

    let window = buffer
        .read_aligned_time_range(&column_keys, 1.0, 4.0)
        .unwrap();

    assert_eq!(window.timestamps, vec![1.0, 2.0, 3.0, 4.0]);
    assert_eq!(window.sample_numbers[&stream_key], vec![0, 1, 2, 3]);
}

#[test]
fn read_aligned_time_range_errors_when_request_exceeds_retention() {
    let mut buffer = Buffer::new(4);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(0.0)],
            vec![ColumnData::Float(1.0)],
            vec![ColumnData::Float(2.0)],
            vec![ColumnData::Float(3.0)],
            vec![ColumnData::Float(4.0)],
            vec![ColumnData::Float(5.0)],
        ],
    );

    let err = buffer
        .read_aligned_time_range(&column_keys, 2.0, 5.0)
        .unwrap_err();

    match err {
        ReadError::RequestedRangeExceedsRetention {
            requested_start,
            requested_end,
            available_start,
            available_end,
        } => {
            assert_eq!(requested_start, 2.0);
            assert_eq!(requested_end, 5.0);
            assert_eq!(available_start, 3.0);
            assert_eq!(available_end, 6.0);
        }
        _ => panic!("expected RequestedRangeExceedsRetention error"),
    }
}

#[test]
fn read_aligned_time_range_errors_when_range_has_no_samples() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(0.0)],
            vec![ColumnData::Float(1.0)],
            vec![ColumnData::Float(2.0)],
            vec![ColumnData::Float(3.0)],
        ],
    );

    let err = buffer
        .read_aligned_time_range(&column_keys, 2.1, 2.9)
        .unwrap_err();

    match err {
        ReadError::NoDataInTimeRange {
            requested_start,
            requested_end,
        } => {
            assert_eq!(requested_start, 2.1);
            assert_eq!(requested_end, 2.9);
        }
        _ => panic!("expected NoDataInTimeRange error"),
    }
}

#[test]
fn read_aligned_time_range_preserves_column_batch_types() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64, DataType::Int64, DataType::UInt64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![
                ColumnData::Float(0.5),
                ColumnData::Int(-1),
                ColumnData::UInt(10),
            ],
            vec![
                ColumnData::Float(1.5),
                ColumnData::Int(-2),
                ColumnData::UInt(11),
            ],
            vec![
                ColumnData::Float(2.5),
                ColumnData::Int(-3),
                ColumnData::UInt(12),
            ],
        ],
    );

    let window = buffer
        .read_aligned_time_range(&column_keys, 1.0, 3.0)
        .unwrap();

    assert_eq!(window.timestamps, vec![1.0, 2.0, 3.0]);
    assert_eq!(window.sample_numbers[&stream_key], vec![0, 1, 2]);

    match &window.columns[&column_keys[0]] {
        ColumnBatch::F64(values) => assert_eq!(values, &vec![0.5, 1.5, 2.5]),
        _ => panic!("expected f64 batch"),
    }
    match &window.columns[&column_keys[1]] {
        ColumnBatch::I64(values) => assert_eq!(values, &vec![-1, -2, -3]),
        _ => panic!("expected i64 batch"),
    }
    match &window.columns[&column_keys[2]] {
        ColumnBatch::U64(values) => assert_eq!(values, &vec![10, 11, 12]),
        _ => panic!("expected u64 batch"),
    }
}

#[test]
fn read_from_cursor_returns_samples_after_cursor() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            vec![ColumnData::Float(0.0)],
            vec![ColumnData::Float(1.0)],
            vec![ColumnData::Float(2.0)],
            vec![ColumnData::Float(3.0)],
            vec![ColumnData::Float(4.0)],
            vec![ColumnData::Float(5.0)],
        ],
    );

    let run_id = buffer.get_run(&stream_key).unwrap().run_id;
    let mut cursors = HashMap::new();
    cursors.insert(
        stream_key.clone(),
        CursorPosition {
            run_id,
            last_sample_number: 2,
        },
    );

    let window = buffer.read_from_cursor(&column_keys, &cursors, 2).unwrap();
    assert_eq!(window.sample_numbers[&stream_key], vec![3, 4]);
    assert_eq!(window.timestamps, vec![4.0, 5.0]);
    match &window.columns[&column_keys[0]] {
        ColumnBatch::F64(values) => assert_eq!(values, &vec![3.0, 4.0]),
        _ => panic!("expected f64 batch"),
    }
}

#[test]
fn read_from_cursor_handles_wrapped_sample_numbers() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    push_rows_with_sample_numbers(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[
            (u32::MAX - 3, vec![ColumnData::Float(10.0)]),
            (u32::MAX - 2, vec![ColumnData::Float(11.0)]),
            (u32::MAX - 1, vec![ColumnData::Float(12.0)]),
            (0, vec![ColumnData::Float(13.0)]),
            (1, vec![ColumnData::Float(14.0)]),
            (2, vec![ColumnData::Float(15.0)]),
        ],
    );

    let run_id = buffer.get_run(&stream_key).unwrap().run_id;
    let mut cursors = HashMap::new();
    cursors.insert(
        stream_key.clone(),
        CursorPosition {
            run_id,
            last_sample_number: u32::MAX - 1,
        },
    );

    let window = buffer.read_from_cursor(&column_keys, &cursors, 2).unwrap();
    assert_eq!(window.sample_numbers[&stream_key], vec![0, 1]);
    match &window.columns[&column_keys[0]] {
        ColumnBatch::F64(values) => assert_eq!(values, &vec![13.0, 14.0]),
        _ => panic!("expected f64 batch"),
    }
}

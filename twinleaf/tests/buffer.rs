use std::sync::Arc;

use twinleaf::data::{
    Boundary, BoundaryReason, Buffer, ColumnData, ColumnVec, ColumnWindow, SampleBatch, Series,
};
use twinleaf::tio::proto::identifiers::{ColumnKey, SampleNumber, StreamKey};
use twinleaf::tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataEpoch, MetadataFilter, SegmentMetadata, StreamMetadata,
};
use twinleaf::tio::proto::{DataType, DeviceRoute};

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
    let numbered: Vec<(SampleNumber, Vec<ColumnData>)> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (i as SampleNumber, row.clone()))
        .collect();
    push_rows_with_sample_numbers(
        buffer, stream_key, columns, device, stream, segment, None, &numbered,
    );
}

#[allow(clippy::too_many_arguments)]
fn push_rows_with_sample_numbers(
    buffer: &mut Buffer,
    stream_key: &StreamKey,
    columns: &[Arc<ColumnMetadata>],
    device: &Arc<DeviceMetadata>,
    stream: &Arc<StreamMetadata>,
    segment: &Arc<SegmentMetadata>,
    boundary: Option<Boundary>,
    rows: &[(SampleNumber, Vec<ColumnData>)],
) {
    let sample_numbers: Vec<SampleNumber> = rows.iter().map(|(n, _)| *n).collect();
    let value_rows: Vec<&[ColumnData]> = rows.iter().map(|(_, row)| row.as_slice()).collect();
    for row in &value_rows {
        assert_eq!(row.len(), columns.len());
    }

    let series = columns
        .iter()
        .enumerate()
        .map(|(ci, meta)| {
            let mut values = ColumnVec::empty_for(meta.data_type.buffer_type());
            for row in &value_rows {
                values.push_data(&row[ci]);
            }
            Series {
                index: meta.index,
                metadata: meta.clone(),
                values,
            }
        })
        .collect();

    let batch = SampleBatch::new(
        DeviceRoute::root(),
        boundary,
        sample_numbers,
        series,
        segment.clone(),
        stream.clone(),
        device.clone(),
    );
    buffer.process_batch(&batch, stream_key.clone());
}

#[test]
fn column_window_time_range_borrows_in_range() {
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

    let w = buffer
        .column_window_time_range(&column_keys[0], 2.0, 4.0)
        .unwrap();
    let (ta, tb) = w.timestamps;
    let ts: Vec<f64> = ta.iter().chain(tb.iter()).copied().collect();
    assert_eq!(ts, vec![2.0, 3.0, 4.0]);
    match w.values.to_owned() {
        ColumnVec::F64(v) => assert_eq!(v, vec![1.0, 2.0, 3.0]),
        _ => panic!("expected f64 column batch"),
    }

    // Reversed bounds normalize to the same window.
    let w2 = buffer
        .column_window_time_range(&column_keys[0], 4.0, 2.0)
        .unwrap();
    let (ta2, tb2) = w2.timestamps;
    let ts2: Vec<f64> = ta2.iter().chain(tb2.iter()).copied().collect();
    assert_eq!(ts2, vec![2.0, 3.0, 4.0]);

    // A range with no samples yields None (so the caller draws nothing).
    assert!(buffer
        .column_window_time_range(&column_keys[0], 100.0, 200.0)
        .is_none());
}

fn window_owned(win: &ColumnWindow) -> (Vec<f64>, Vec<f64>) {
    let (ta, tb) = win.timestamps;
    let mut ts = Vec::with_capacity(ta.len() + tb.len());
    ts.extend_from_slice(ta);
    ts.extend_from_slice(tb);
    let vals = match win.values.to_owned() {
        ColumnVec::F64(v) => v,
        other => panic!("expected f64 batch, got {other:?}"),
    };
    (ts, vals)
}

fn fill_floats(buffer: &mut Buffer, count: usize) -> (StreamKey, Vec<ColumnKey>) {
    let (stream_key, columns, column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);
    let rows: Vec<_> = (0..count)
        .map(|i| vec![ColumnData::Float(i as f64)])
        .collect();
    push_rows(
        buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &rows,
    );
    (stream_key, column_keys)
}

#[test]
fn column_window_last_n_empty_returns_none() {
    let buffer = Buffer::new(8);
    let (_stream_key, _cols, column_keys, _d, _s, _seg) = test_fixture(&[DataType::Float64]);
    assert!(buffer.column_window_last_n(&column_keys[0], 4).is_none());
}

#[test]
fn column_window_last_n_across_ring_seam() {
    let mut buffer = Buffer::new(5);
    let (_stream_key, column_keys) = fill_floats(&mut buffer, 12);
    let col = &column_keys[0];

    // Values are the sample index; timestamps are the sample-period end (index + 1).

    // Full window straddling the seam.
    let win = buffer.column_window_last_n(col, 5).unwrap();
    let (ts, vals) = window_owned(&win);
    assert_eq!(vals, vec![7.0, 8.0, 9.0, 10.0, 11.0]);
    assert_eq!(ts, vec![8.0, 9.0, 10.0, 11.0, 12.0]);
    assert_eq!(win.values.len(), 5);

    // Window entirely within the second physical half (last two samples).
    let win = buffer.column_window_last_n(col, 2).unwrap();
    let (ts, vals) = window_owned(&win);
    assert_eq!(vals, vec![10.0, 11.0]);
    assert_eq!(ts, vec![11.0, 12.0]);

    // Window straddling the seam with count < len (three samples).
    let win = buffer.column_window_last_n(col, 3).unwrap();
    let (ts, vals) = window_owned(&win);
    assert_eq!(vals, vec![9.0, 10.0, 11.0]);
    assert_eq!(ts, vec![10.0, 11.0, 12.0]);

    // count < len, leaving a sample dropped from the front of the window.
    let win = buffer.column_window_last_n(col, 4).unwrap();
    let (ts, vals) = window_owned(&win);
    assert_eq!(vals, vec![8.0, 9.0, 10.0, 11.0]);
    assert_eq!(ts, vec![9.0, 10.0, 11.0, 12.0]);

    // n larger than retained length is clamped to len.
    let win = buffer.column_window_last_n(col, 100).unwrap();
    let (ts, vals) = window_owned(&win);
    assert_eq!(vals, vec![7.0, 8.0, 9.0, 10.0, 11.0]);
    assert_eq!(ts, vec![8.0, 9.0, 10.0, 11.0, 12.0]);
}

#[test]
fn column_window_last_n_seam_sweep() {
    let cap = 5usize;
    for total in 1..=30usize {
        let mut buffer = Buffer::new(cap);
        let (_stream_key, column_keys) = fill_floats(&mut buffer, total);
        let col = &column_keys[0];

        let len = total.min(cap);
        for n in 1..=(cap + 2) {
            let win = buffer.column_window_last_n(col, n).unwrap();
            let (ts, vals) = window_owned(&win);
            let count = n.min(len);
            let expected_vals: Vec<f64> = ((total - count)..total).map(|i| i as f64).collect();
            let expected_ts: Vec<f64> = ((total - count)..total).map(|i| (i + 1) as f64).collect();
            assert_eq!(ts, expected_ts, "total={total} n={n} timestamps");
            assert_eq!(vals, expected_vals, "total={total} n={n} values");
            assert_eq!(win.values.len(), count);
        }
    }
}

#[test]
fn latest_row_returns_newest_values_and_metadata() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, _column_keys, device, stream, segment) =
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
        ],
    );

    // A later, separate batch must overwrite the "latest" values.
    push_rows_with_sample_numbers(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        None,
        &[(
            2,
            vec![
                ColumnData::Float(2.5),
                ColumnData::Int(-3),
                ColumnData::UInt(12),
            ],
        )],
    );

    let row = buffer.latest_row(&stream_key).unwrap();
    assert!(Arc::ptr_eq(&row.stream, &stream));
    assert!(Arc::ptr_eq(&row.segment, &segment));
    assert!(row.last_seen.elapsed() < std::time::Duration::from_secs(5));

    // Ordered by column id, with the newest value on each column.
    assert_eq!(row.columns.len(), 3);
    assert_eq!(row.columns[0].0.data_type, DataType::Float64);
    assert_eq!(row.columns[1].0.data_type, DataType::Int64);
    assert_eq!(row.columns[2].0.data_type, DataType::UInt64);
    match row.columns[0].1 {
        ColumnData::Float(v) => assert_eq!(v, 2.5),
        ref other => panic!("expected float, got {other:?}"),
    }
    match row.columns[1].1 {
        ColumnData::Int(v) => assert_eq!(v, -3),
        ref other => panic!("expected int, got {other:?}"),
    }
    match row.columns[2].1 {
        ColumnData::UInt(v) => assert_eq!(v, 12),
        ref other => panic!("expected uint, got {other:?}"),
    }

    // No active run for a stream that never received data.
    let other = StreamKey::new(DeviceRoute::root(), 99);
    assert!(buffer.latest_row(&other).is_none());
}

#[test]
fn discontinuous_boundary_starts_a_new_run() {
    let mut buffer = Buffer::new(16);
    let (stream_key, columns, _column_keys, device, stream, segment) =
        test_fixture(&[DataType::Float64]);

    // First batch establishes the run. A `None` boundary is continuous, so the
    // run id stays put across a following continuous batch.
    push_rows(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        &[vec![ColumnData::Float(0.0)], vec![ColumnData::Float(1.0)]],
    );
    let first_run = buffer.get_run(&stream_key).unwrap().run_id;

    push_rows_with_sample_numbers(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        None,
        &[(2, vec![ColumnData::Float(2.0)])],
    );
    assert_eq!(
        buffer.get_run(&stream_key).unwrap().run_id,
        first_run,
        "a continuous batch must not split the run"
    );

    // A discontinuous boundary forces a new run, replacing the old `continuous`
    // argument that the pre-SoA API took explicitly.
    push_rows_with_sample_numbers(
        &mut buffer,
        &stream_key,
        &columns,
        &device,
        &stream,
        &segment,
        Some(Boundary {
            reason: BoundaryReason::SegmentChanged {
                old_id: 0,
                new_id: 1,
            },
        }),
        &[(3, vec![ColumnData::Float(3.0)])],
    );
    assert_ne!(
        buffer.get_run(&stream_key).unwrap().run_id,
        first_run,
        "a discontinuous boundary must start a new run"
    );
}

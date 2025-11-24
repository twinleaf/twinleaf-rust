use std::collections::HashMap;

use crate::{data::{AlignedWindow, ReadError, buffer::{ActiveSegment, SegmentWindow}}, device::{ColumnSpec, StreamKey}};

pub fn validate_sampling_rates(
    stream_windows: &HashMap<StreamKey, (SegmentWindow, &ActiveSegment)>,
) -> Result<(), ReadError> {
    if stream_windows.len() <= 1 {
        return Ok(());
    }

    let rates: Vec<f64> = stream_windows
        .values()
        .map(|(_, active)| {
            (active.buffer.segment_metadata.sampling_rate
                / active.buffer.segment_metadata.decimation) as f64
        })
        .collect();

    let first_rate = rates[0];
    let all_match = rates.iter().all(|r| (r - first_rate).abs() < 1e-6);

    if !all_match {
        return Err(ReadError::SamplingRateMismatch {
            streams: stream_windows.keys().cloned().collect(),
            rates,
        });
    }

    Ok(())
}

pub fn validate_stream_alignment(
    stream_windows: &HashMap<StreamKey, (SegmentWindow, &ActiveSegment)>,
) -> Result<(), ReadError> {
    if stream_windows.len() <= 1 {
        return Ok(());
    }

    let mut windows_iter = stream_windows.iter();
    let (_first_key, (first_window, _)) = windows_iter.next().unwrap();
    let expected_first = *first_window.sample_numbers.first().unwrap();
    let expected_last = *first_window.sample_numbers.last().unwrap();

    for (stream_key, (window, _)) in windows_iter {
        let first = *window.sample_numbers.first().unwrap();
        let last = *window.sample_numbers.last().unwrap();

        if first != expected_first || last != expected_last {
            return Err(ReadError::SampleNumberMismatch {
                streams: stream_windows.keys().cloned().collect(),
                reason: format!(
                    "Stream {:?} has samples [{}, {}], expected [{}, {}]",
                    stream_key, first, last, expected_first, expected_last
                ),
            });
        }
    }

    Ok(())
}

pub fn merge_windows(
    stream_windows: HashMap<StreamKey, (SegmentWindow, &ActiveSegment)>,
    by_stream: HashMap<StreamKey, Vec<&ColumnSpec>>,
) -> Result<AlignedWindow, ReadError> {
    let (first_window, _) = stream_windows.values().next().unwrap();
    
    let sample_numbers = first_window.sample_numbers.clone();
    let timestamps = first_window.timestamps.clone();

    let mut columns = HashMap::new();
    let mut stream_metadata = HashMap::new();
    let mut segment_metadata = HashMap::new();
    let mut column_metadata = HashMap::new();
    let mut session_ids = HashMap::new();

    for (stream_key, (window, active)) in stream_windows {
        let requested_specs = by_stream.get(&stream_key).unwrap();


        stream_metadata.insert(stream_key.clone(), active.buffer.stream_metadata.clone());
        segment_metadata.insert(stream_key.clone(), active.buffer.segment_metadata.clone());
        session_ids.insert(stream_key.clone(), active.session_id);

        for &col_spec in requested_specs {
            if let Some(data) = window.columns.get(&col_spec.column_id) {
                columns.insert(col_spec.clone(), data.clone());
            }

            if let Some(col_buffer) = active.buffer.columns.get(&col_spec.column_id) {
                column_metadata.insert(col_spec.clone(), col_buffer.metadata().clone());
            }
        }
    }

    Ok(AlignedWindow {
        sample_numbers,
        timestamps,
        columns,
        stream_metadata,
        segment_metadata,
        column_metadata,
        session_ids,
    })
}
use crate::data::buffer::{AlignedWindow, DataSlice, ReadError};
use crate::tio::proto::identifiers::ColumnKey;
use std::collections::HashMap;

pub fn validate_sampling_rates(slices: &[DataSlice]) -> Result<(), ReadError> {
    if slices.len() <= 1 {
        return Ok(());
    }

    let rates: Vec<f64> = slices
        .iter()
        .map(|s| (s.segment_metadata.sampling_rate as f64) / (s.segment_metadata.decimation as f64))
        .collect();

    let first_rate = rates[0];
    let all_match = rates.iter().all(|r| (r - first_rate).abs() < 1e-6);

    if !all_match {
        return Err(ReadError::SamplingRateMismatch {
            streams: slices.iter().map(|s| s.stream_key.clone()).collect(),
            rates,
        });
    }

    Ok(())
}

pub fn validate_stream_alignment(slices: &[DataSlice]) -> Result<(), ReadError> {
    if slices.len() <= 1 {
        return Ok(());
    }

    let first_slice = &slices[0];
    let expected_len = first_slice.sample_numbers.len();
    
    if expected_len == 0 {
        return Ok(());
    }

    let expected_first = first_slice.sample_numbers[0];
    let expected_last = first_slice.sample_numbers[expected_len - 1];

    for slice in &slices[1..] {
        if slice.sample_numbers.len() != expected_len {
            return Err(ReadError::SampleNumberMismatch {
                streams: slices.iter().map(|s| s.stream_key.clone()).collect(),
                reason: format!("Length mismatch: {} vs {}", expected_len, slice.len()),
            });
        }

        let first = slice.sample_numbers[0];
        let last = slice.sample_numbers[expected_len - 1];

        if first != expected_first || last != expected_last {
            return Err(ReadError::SampleNumberMismatch {
                streams: slices.iter().map(|s| s.stream_key.clone()).collect(),
                reason: format!(
                    "Stream {:?} has samples [{}, {}], expected [{}, {}]",
                    slice.stream_key, first, last, expected_first, expected_last
                ),
            });
        }
    }

    Ok(())
}


pub fn merge_slices(slices: Vec<DataSlice>) -> Result<AlignedWindow, ReadError> {
    if slices.is_empty() {
        return Err(ReadError::NoColumnsRequested);
    }

    validate_sampling_rates(&slices)?;
    validate_stream_alignment(&slices)?;

    let first_slice = &slices[0];
    let sample_numbers = first_slice.sample_numbers.clone();
    let timestamps = first_slice.timestamps.clone();

    let mut columns = HashMap::new();
    let mut stream_metadata = HashMap::new();
    let mut segment_metadata = HashMap::new();
    let mut column_metadata = HashMap::new();
    let mut session_ids = HashMap::new();

    for slice in slices {
        let sk = slice.stream_key;

        stream_metadata.insert(sk.clone(), slice.stream_metadata);
        segment_metadata.insert(sk.clone(), slice.segment_metadata);
        session_ids.insert(sk.clone(), slice.session_id);

        for (col_id, batch) in slice.columns {
            let key = ColumnKey::new(
                sk.route.clone(), 
                sk.stream_id, 
                col_id
            );
            columns.insert(key, batch);
        }

        for (col_id, meta) in slice.column_metadata {
            let key = ColumnKey::new(
                sk.route.clone(), 
                sk.stream_id, 
                col_id
            );
            column_metadata.insert(key, meta);
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
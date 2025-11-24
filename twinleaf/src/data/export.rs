use crate::data::buffer::{AlignedWindow, ColumnBatch};
use crate::device::{ColumnSpec, StreamKey};
use crate::tio::proto::meta::{ColumnMetadata, SegmentMetadata};
use hdf5::{File, Result, H5Type};
use hdf5::types::VarLenUnicode;
use std::collections::HashMap;
use std::path::Path;

pub fn export_window(path: &Path, window: &AlignedWindow) -> Result<()> {
    let file = File::create(path)?;

    let mut columns_by_stream: HashMap<StreamKey, Vec<&ColumnSpec>> = HashMap::new();
    for spec in window.columns.keys() {
        columns_by_stream.entry(spec.stream_key()).or_default().push(spec);
    }

    for (stream_key, col_specs) in columns_by_stream {
        let (route, stream_id) = &stream_key;

        let raw_route = route.to_string();
        let route_str = raw_route.trim_start_matches('/');
        
        let stream_name = window.stream_metadata.get(&stream_key)
            .map(|m| m.name.clone())
            .unwrap_or_else(|| format!("stream_{}", stream_id));

        let group_path = format!("/{}/{}", route_str, stream_name);
        let group = file.create_group(&group_path)?;

        if let Some(seg_meta) = window.segment_metadata.get(&stream_key) {
            write_segment_attributes(&group, seg_meta)?;
        }

        let time_ds = group.new_dataset::<f64>()
            .shape((window.timestamps.len(),))
            .chunk((window.timestamps.len(),)) 
            .deflate(3)
            .create("time")?;
        time_ds.write(&window.timestamps)?;

        for spec in col_specs {
            if let Some(batch) = window.columns.get(spec) {
                let col_meta = window.column_metadata.get(spec);
                let dataset_name = col_meta
                    .map(|m| m.name.clone())
                    .unwrap_or_else(|| format!("col_{}", spec.column_id));

                match batch {
                    ColumnBatch::F64(data) => write_ds(&group, &dataset_name, data, col_meta)?,
                    ColumnBatch::I64(data) => write_ds(&group, &dataset_name, data, col_meta)?,
                    ColumnBatch::U64(data) => write_ds(&group, &dataset_name, data, col_meta)?,
                }
            }
        }
    }
    Ok(())
}

fn write_segment_attributes(group: &hdf5::Group, meta: &SegmentMetadata) -> Result<()> {
    group.new_attr::<u32>().create("sampling_rate")?.write_scalar(&meta.sampling_rate)?;
    group.new_attr::<u32>().create("decimation")?.write_scalar(&meta.decimation)?;
    Ok(())
}

fn write_ds<T: H5Type + Clone>(
    group: &hdf5::Group,
    name: &str,
    data: &[T],
    meta: Option<&std::sync::Arc<ColumnMetadata>>,
) -> Result<()> {
    let ds = group.new_dataset::<T>()
        .shape((data.len(),))
        .chunk((data.len(),))
        .deflate(3)
        .create(name)?;
    ds.write(data)?;

    if let Some(m) = meta {
        if !m.units.is_empty() {
            let attr = ds.new_attr::<VarLenUnicode>().create("unit")?;
            attr.write_scalar(&m.units.parse::<VarLenUnicode>().unwrap())?;
        }
        if !m.description.is_empty() {
            let attr = ds.new_attr::<VarLenUnicode>().create("description")?;
            attr.write_scalar(&m.description.parse::<VarLenUnicode>().unwrap())?;
        }
    }
    Ok(())
}
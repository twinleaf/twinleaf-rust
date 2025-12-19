use crate::data::sample::Sample;
use crate::data::ColumnFilter;
use crate::tio::proto::identifiers::{ColumnId, SampleNumber, StreamKey};
use crate::tio::proto::{BufferType, ColumnMetadata, SegmentMetadata, StreamMetadata};
use hdf5::filters::{Blosc, BloscShuffle};
use hdf5::types::VarLenUnicode;
use hdf5::{Dataset, File, Group, H5Type, Location, Result, SimpleExtents};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

pub type RunId = u64;

/// Controls when to start a new run in the output file.
#[derive(Debug, Clone, Copy, Default)]
pub enum SplitPolicy {
    /// Split on any discontinuity (default)
    #[default]
    Continuous,
    /// Only split on non-monotonic breaks (allows gaps)
    Monotonic,
}

#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    pub total_samples: u64,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
    pub streams_written: HashSet<String>,
}

enum ColumnBatch {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

struct PendingBatch {
    run_id: RunId,
    sample_numbers: Vec<SampleNumber>,
    timestamps: Vec<f64>,
    columns: HashMap<ColumnId, ColumnBatch>,
    stream_metadata: Arc<StreamMetadata>,
    segment_metadata: Arc<SegmentMetadata>,
    column_metadata: HashMap<ColumnId, Arc<ColumnMetadata>>,
    session_id: u32,
    is_first_chunk: bool,
}

impl PendingBatch {
    fn new(sample: &Sample, run_id: RunId) -> Self {
        Self {
            run_id,
            sample_numbers: Vec::new(),
            timestamps: Vec::new(),
            columns: HashMap::new(),
            stream_metadata: sample.stream.clone(),
            segment_metadata: sample.segment.clone(),
            column_metadata: HashMap::new(),
            session_id: sample.device.session_id,
            is_first_chunk: true,
        }
    }

    fn len(&self) -> usize {
        self.timestamps.len()
    }

    fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    fn push(&mut self, sample: &Sample) {
        use crate::data::sample::ColumnData;

        self.sample_numbers.push(sample.n);
        self.timestamps.push(sample.timestamp_end());
        self.segment_metadata = sample.segment.clone();

        for col in &sample.columns {
            let col_id = col.desc.index as ColumnId;

            self.column_metadata
                .entry(col_id)
                .or_insert_with(|| col.desc.clone());

            let batch = self.columns.entry(col_id).or_insert_with(|| {
                match col.desc.data_type.buffer_type() {
                    BufferType::Float => ColumnBatch::F64(Vec::new()),
                    BufferType::Int => ColumnBatch::I64(Vec::new()),
                    BufferType::UInt => ColumnBatch::U64(Vec::new()),
                }
            });

            match (batch, &col.value) {
                (ColumnBatch::F64(v), ColumnData::Float(val)) => v.push(*val),
                (ColumnBatch::F64(v), ColumnData::Int(val)) => v.push(*val as f64),
                (ColumnBatch::I64(v), ColumnData::Int(val)) => v.push(*val),
                (ColumnBatch::U64(v), ColumnData::UInt(val)) => v.push(*val),
                _ => {}
            }
        }
    }

    fn drain(&mut self) -> PendingBatch {
        let batch = PendingBatch {
            run_id: self.run_id,
            sample_numbers: std::mem::take(&mut self.sample_numbers),
            timestamps: std::mem::take(&mut self.timestamps),
            columns: std::mem::take(&mut self.columns),
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
            column_metadata: std::mem::take(&mut self.column_metadata),
            session_id: self.session_id,
            is_first_chunk: self.is_first_chunk,
        };
        self.is_first_chunk = false;
        batch
    }
}

pub struct Hdf5Appender {
    file: File,
    datasets: HashMap<String, Dataset>,
    pending: HashMap<StreamKey, PendingBatch>,
    filter: Option<ColumnFilter>,
    compress: bool,
    debug: bool,
    batch_size: usize,
    split_policy: SplitPolicy,
    seen_debug: HashSet<String>,
    stats: ExportStats,
}

impl Hdf5Appender {
    pub fn new(
        path: &Path,
        compress: bool,
        debug: bool,
        filter: Option<ColumnFilter>,
        batch_size: usize,
    ) -> Result<Self> {
        Self::with_policy(
            path,
            compress,
            debug,
            filter,
            batch_size,
            SplitPolicy::default(),
        )
    }

    pub fn with_policy(
        path: &Path,
        compress: bool,
        debug: bool,
        filter: Option<ColumnFilter>,
        batch_size: usize,
        split_policy: SplitPolicy,
    ) -> Result<Self> {
        if path.exists() {
            panic!("Refusing to overwrite existing file: {:?}", path);
        }

        Ok(Self {
            file: File::create(path)?,
            datasets: HashMap::new(),
            pending: HashMap::new(),
            filter,
            compress,
            debug,
            batch_size,
            split_policy,
            seen_debug: HashSet::new(),
            stats: ExportStats::default(),
        })
    }

    pub fn write_sample(&mut self, sample: Sample, key: StreamKey) -> Result<()> {
        let should_split = match self.split_policy {
            SplitPolicy::Continuous => !sample.is_continuous(),
            SplitPolicy::Monotonic => !sample.is_monotonic(),
        };

        if should_split {
            self.flush_and_advance_run(&key)?;
        }

        if !self.pending.contains_key(&key) {
            self.pending
                .insert(key.clone(), PendingBatch::new(&sample, 0));
        }

        self.pending.get_mut(&key).unwrap().push(&sample);

        if self.pending.get(&key).unwrap().len() >= self.batch_size {
            self.flush_stream(&key)?;
        }

        Ok(())
    }

    fn flush_and_advance_run(&mut self, key: &StreamKey) -> Result<()> {
        if let Some(mut batch) = self.pending.remove(key) {
            if !batch.is_empty() {
                let drained = batch.drain();
                self.write_batch(key, drained)?;
            }
            batch.run_id += 1;
            batch.is_first_chunk = true;
            self.pending.insert(key.clone(), batch);
        }
        Ok(())
    }

    fn flush_stream(&mut self, key: &StreamKey) -> Result<()> {
        if let Some(batch) = self.pending.get_mut(key) {
            if !batch.is_empty() {
                let drained = batch.drain();
                self.write_batch(key, drained)?;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ExportStats> {
        let keys: Vec<_> = self.pending.keys().cloned().collect();
        for key in keys {
            self.flush_stream(&key)?;
        }
        Ok(self.stats)
    }

    fn write_batch(&mut self, key: &StreamKey, batch: PendingBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let route_str = key.route.to_string().trim_start_matches('/').to_string();
        let stream_name = &batch.stream_metadata.name;

        let valid_columns: Vec<_> = batch
            .columns
            .iter()
            .filter_map(|(col_id, col_batch)| {
                let meta = batch.column_metadata.get(col_id)?;
                let name = &meta.name;

                if let Some(f) = &self.filter {
                    let path = f.get_path_string(&key.route, stream_name, name);
                    if self.debug && self.seen_debug.insert(path.clone()) {
                        println!(
                            "[DEBUG] Filter: '{}' -> {}",
                            path,
                            f.matches(&key.route, stream_name, name)
                        );
                    }
                    if !f.matches(&key.route, stream_name, name) {
                        return None;
                    }
                }

                Some((col_id, col_batch, meta))
            })
            .collect();

        if valid_columns.is_empty() {
            return Ok(());
        }

        let group_path = if route_str.is_empty() {
            format!("/run_{:06}/{}", batch.run_id, stream_name)
        } else {
            format!("/{}/run_{:06}/{}", route_str, batch.run_id, stream_name)
        };

        self.ensure_group(&group_path)?;
        let group = self.file.group(&group_path)?;

        if batch.is_first_chunk {
            self.write_metadata_attributes(&group, &batch)?;
        }

        self.append_dataset(
            &group_path,
            "sample_number",
            &batch.sample_numbers,
            None,
            Some("Sample number from device"),
        )?;

        self.append_dataset(
            &group_path,
            "time",
            &batch.timestamps,
            None,
            Some("Time in seconds"),
        )?;

        for (_, col_batch, meta) in valid_columns {
            let units = Some(&meta.units).filter(|u| !u.is_empty());
            let desc = Some(meta.description.as_str()).filter(|d| !d.is_empty());

            match col_batch {
                ColumnBatch::F64(data) => {
                    self.append_dataset(&group_path, &meta.name, data, units, desc)?
                }
                ColumnBatch::I64(data) => {
                    self.append_dataset(&group_path, &meta.name, data, units, desc)?
                }
                ColumnBatch::U64(data) => {
                    self.append_dataset(&group_path, &meta.name, data, units, desc)?
                }
            }
        }

        self.stats.total_samples += batch.len() as u64;
        if let (Some(&first), Some(&last)) = (batch.timestamps.first(), batch.timestamps.last()) {
            self.stats.start_time = Some(self.stats.start_time.map_or(first, |t| t.min(first)));
            self.stats.end_time = Some(self.stats.end_time.map_or(last, |t| t.max(last)));
        }
        let stream_path = if route_str.is_empty() {
            format!("/{}", stream_name)
        } else {
            format!("/{}/{}", route_str, stream_name)
        };
        self.stats.streams_written.insert(stream_path);

        Ok(())
    }

    fn write_metadata_attributes(&self, group: &Group, batch: &PendingBatch) -> Result<()> {
        let meta = &batch.segment_metadata;
        self.write_attr_scalar(group, "sampling_rate", &meta.sampling_rate)?;
        self.write_attr_scalar(group, "decimation", &meta.decimation)?;
        self.write_attr_scalar(group, "start_time", &meta.start_time)?;
        self.write_attr_scalar(group, "filter_cutoff", &meta.filter_cutoff)?;
        self.write_attr_scalar(group, "session_id", &batch.session_id)?;
        self.write_attr_scalar(group, "run_id", &batch.run_id)?;

        let epoch_u8: u8 = meta.time_ref_epoch.clone().into();
        self.write_attr_scalar(group, "time_ref_epoch", &epoch_u8)?;

        let filter_type_u8: u8 = meta.filter_type.clone().into();
        self.write_attr_scalar(group, "filter_type", &filter_type_u8)?;

        if !meta.time_ref_serial.is_empty() {
            self.write_attr_string(group, "time_ref_serial", &meta.time_ref_serial)?;
        }
        Ok(())
    }

    fn append_dataset<T: H5Type + Clone>(
        &mut self,
        group_path: &str,
        name: &str,
        data: &[T],
        units: Option<&String>,
        description: Option<&str>,
    ) -> Result<()> {
        let full_path = format!("{}/{}", group_path, name);

        if !self.datasets.contains_key(&full_path) {
            let group = self.file.group(group_path)?;
            let ds = if let Ok(existing) = group.dataset(name) {
                existing
            } else {
                let builder = group
                    .new_dataset::<T>()
                    .chunk((65_536,))
                    .shape(SimpleExtents::resizable([0usize]));
                let builder = if self.compress {
                    builder.blosc(Blosc::BloscLZ, 5, BloscShuffle::Byte)
                } else {
                    builder
                };
                let ds = builder.create(name)?;
                if let Some(u) = units {
                    self.write_attr_string(&ds, "units", u)?;
                }
                if let Some(d) = description {
                    self.write_attr_string(&ds, "description", d)?;
                }
                ds
            };
            self.datasets.insert(full_path.clone(), ds);
        }

        let ds = self.datasets.get(&full_path).unwrap();
        let current_size = ds.shape()[0];
        ds.resize((current_size + data.len(),))?;
        ds.write_slice(data, current_size..)?;
        Ok(())
    }

    fn ensure_group(&self, path: &str) -> Result<()> {
        if self.file.group(path).is_ok() {
            return Ok(());
        }
        let mut current = String::new();
        for part in path.split('/').filter(|s| !s.is_empty()) {
            current.push('/');
            current.push_str(part);
            if self.file.group(&current).is_err() {
                self.file.create_group(&current)?;
            }
        }
        Ok(())
    }

    fn write_attr_scalar<T: H5Type>(&self, loc: &Location, name: &str, val: &T) -> Result<()> {
        loc.new_attr::<T>().create(name)?.write_scalar(val)
    }

    fn write_attr_string(&self, loc: &Location, name: &str, val: &str) -> Result<()> {
        let attr = loc.new_attr::<VarLenUnicode>().create(name)?;
        attr.write_scalar(&val.parse::<VarLenUnicode>().unwrap())
    }
}

use crate::data::sample::Sample;
use crate::data::ColumnFilter;
use crate::tio::proto::identifiers::{ColumnId, DeviceRoute, SampleNumber, StreamKey};
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

/// Controls the granularity of run splitting in the HDF5 output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunSplitLevel {
    /// No run splitting - flat structure: /{route}/{stream}/{datasets}
    #[default]
    None,
    /// Each stream has independent run counter: /{route}/{stream}/run_{id}/{datasets}
    PerStream,
    /// All streams on a device share run counter: /{route}/run_{id}/{stream}/{datasets}
    PerDevice,
    /// All streams globally share run counter: /run_{id}/{route}/{stream}/{datasets}
    Global,
}

#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    pub total_samples: u64,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
    pub streams_written: HashSet<String>,
    pub streams_seen: HashSet<String>,
    pub discontinuities_detected: u64,
}

enum ColumnBatch {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

struct PendingBatch {
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
    fn new(sample: &Sample) -> Self {
        Self {
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
    split_level: RunSplitLevel,
    stream_runs: HashMap<StreamKey, RunId>,
    device_runs: HashMap<DeviceRoute, RunId>,
    global_run: RunId,
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
        Self::with_options(
            path,
            compress,
            debug,
            filter,
            batch_size,
            SplitPolicy::default(),
            RunSplitLevel::default(),
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
        Self::with_options(
            path,
            compress,
            debug,
            filter,
            batch_size,
            split_policy,
            RunSplitLevel::default(),
        )
    }

    pub fn with_options(
        path: &Path,
        compress: bool,
        debug: bool,
        filter: Option<ColumnFilter>,
        batch_size: usize,
        split_policy: SplitPolicy,
        split_level: RunSplitLevel,
    ) -> Result<Self> {
        Ok(Self {
            file: File::create(path)?,
            datasets: HashMap::new(),
            pending: HashMap::new(),
            filter,
            compress,
            debug,
            batch_size,
            split_policy,
            split_level,
            stream_runs: HashMap::new(),
            device_runs: HashMap::new(),
            global_run: 0,
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
            self.handle_discontinuity(&key)?;
        }

        if !self.pending.contains_key(&key) {
            self.pending.insert(key.clone(), PendingBatch::new(&sample));
        }

        self.pending.get_mut(&key).unwrap().push(&sample);

        if self.pending.get(&key).unwrap().len() >= self.batch_size {
            self.flush_stream(&key)?;
        }

        Ok(())
    }

    fn handle_discontinuity(&mut self, key: &StreamKey) -> Result<()> {
        self.stats.discontinuities_detected += 1;
        match self.split_level {
            RunSplitLevel::None => {
                self.flush_stream(key)?;
            }
            RunSplitLevel::PerStream => {
                self.flush_stream(key)?;
                if let Some(batch) = self.pending.get_mut(key) {
                    batch.is_first_chunk = true;
                }
                *self.stream_runs.entry(key.clone()).or_insert(0) += 1;
            }
            RunSplitLevel::PerDevice => {
                self.flush_all_for_device(&key.route)?;
                *self.device_runs.entry(key.route.clone()).or_insert(0) += 1;
            }
            RunSplitLevel::Global => {
                self.flush_all()?;
                self.global_run += 1;
            }
        }
        Ok(())
    }

    fn flush_all_for_device(&mut self, route: &DeviceRoute) -> Result<()> {
        let keys: Vec<_> = self
            .pending
            .keys()
            .filter(|k| &k.route == route)
            .cloned()
            .collect();
        for key in keys {
            self.flush_stream(&key)?;
            if let Some(batch) = self.pending.get_mut(&key) {
                batch.is_first_chunk = true;
            }
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.pending.keys().cloned().collect();
        for key in keys {
            self.flush_stream(&key)?;
            if let Some(batch) = self.pending.get_mut(&key) {
                batch.is_first_chunk = true;
            }
        }
        Ok(())
    }

    fn make_group_path(&self, route_str: &str, stream_name: &str, key: &StreamKey) -> String {
        match self.split_level {
            RunSplitLevel::None => {
                if route_str.is_empty() {
                    format!("/{}", stream_name)
                } else {
                    format!("/{}/{}", route_str, stream_name)
                }
            }
            RunSplitLevel::PerStream => {
                let run_id = self.stream_runs.get(key).copied().unwrap_or(0);
                if route_str.is_empty() {
                    format!("/{}/run_{:06}", stream_name, run_id)
                } else {
                    format!("/{}/{}/run_{:06}", route_str, stream_name, run_id)
                }
            }
            RunSplitLevel::PerDevice => {
                let run_id = self.device_runs.get(&key.route).copied().unwrap_or(0);
                if route_str.is_empty() {
                    format!("/run_{:06}/{}", run_id, stream_name)
                } else {
                    format!("/{}/run_{:06}/{}", route_str, run_id, stream_name)
                }
            }
            RunSplitLevel::Global => {
                if route_str.is_empty() {
                    format!("/run_{:06}/{}", self.global_run, stream_name)
                } else {
                    format!("/run_{:06}/{}/{}", self.global_run, route_str, stream_name)
                }
            }
        }
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

        let stream_path = if route_str.is_empty() {
            format!("/{}", stream_name)
        } else {
            format!("/{}/{}", route_str, stream_name)
        };
        self.stats.streams_seen.insert(stream_path.clone());

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

        let group_path = self.make_group_path(&route_str, stream_name, key);

        self.ensure_group(&group_path)?;
        let group = self.file.group(&group_path)?;

        if batch.is_first_chunk {
            self.write_metadata_attributes(&group, &batch, key)?;
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
        self.stats.streams_written.insert(stream_path);

        Ok(())
    }

    fn write_metadata_attributes(
        &self,
        group: &Group,
        batch: &PendingBatch,
        key: &StreamKey,
    ) -> Result<()> {
        let meta = &batch.segment_metadata;
        self.write_attr_scalar(group, "sampling_rate", &meta.sampling_rate)?;
        self.write_attr_scalar(group, "decimation", &meta.decimation)?;
        self.write_attr_scalar(group, "start_time", &meta.start_time)?;
        self.write_attr_scalar(group, "filter_cutoff", &meta.filter_cutoff)?;
        self.write_attr_scalar(group, "session_id", &batch.session_id)?;

        let run_id = self.get_run_id(key);
        if let Some(id) = run_id {
            self.write_attr_scalar(group, "run_id", &id)?;
        }

        let epoch_u8: u8 = meta.time_ref_epoch.clone().into();
        self.write_attr_scalar(group, "time_ref_epoch", &epoch_u8)?;

        let filter_type_u8: u8 = meta.filter_type.clone().into();
        self.write_attr_scalar(group, "filter_type", &filter_type_u8)?;

        if !meta.time_ref_serial.is_empty() {
            self.write_attr_string(group, "time_ref_serial", &meta.time_ref_serial)?;
        }
        Ok(())
    }

    fn get_run_id(&self, key: &StreamKey) -> Option<RunId> {
        match self.split_level {
            RunSplitLevel::None => None,
            RunSplitLevel::PerStream => Some(self.stream_runs.get(key).copied().unwrap_or(0)),
            RunSplitLevel::PerDevice => {
                Some(self.device_runs.get(&key.route).copied().unwrap_or(0))
            }
            RunSplitLevel::Global => Some(self.global_run),
        }
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

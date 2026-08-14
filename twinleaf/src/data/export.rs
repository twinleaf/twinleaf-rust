use crate::data::{ColumnFilter, ColumnVec, SampleBatch, Series};
use crate::tio::proto::identifiers::{ColumnId, DeviceRoute, StreamKey};
use hdf5::filters::{Blosc, BloscShuffle};
use hdf5::types::{CompoundField, CompoundType, FloatSize, IntSize, TypeDescriptor, VarLenUnicode};
use hdf5::{Dataset, Dataspace, File, H5Type, Location, Result, SimpleExtents};
use hdf5_sys::h5d::H5Dwrite;
use hdf5_sys::h5p::H5P_DEFAULT;
use std::collections::{HashMap, HashSet};
use std::path::Path;

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
///
/// Every stream is written as a single compound table (with `sample`, `time`,
/// and one field per column) inside the group for its device route. Run
/// splitting controls how discontinuities carve a stream into multiple tables;
/// each run becomes its own table in the same route group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunSplitLevel {
    /// No run splitting - one table per stream: `/{route}/{stream}`
    #[default]
    None,
    /// Each stream has an independent run counter: `/{route}/{stream}_run{id}`
    PerStream,
    /// All streams on a device share a run counter: `/{route}/{stream}_run{id}`
    PerDevice,
    /// All streams globally share a run counter: `/{route}/{stream}_run{id}`
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

/// Where each field of a compound table row gets its value.
#[derive(Clone, Copy)]
enum FieldSource {
    Sample,
    Time,
    Column(ColumnId),
}

/// A compound table (one per stream/run) and the layout needed to pack rows.
struct TableInfo {
    dataset: Dataset,
    row_size: usize,
    /// `(byte offset, source)` for each compound field, in field order.
    fields: Vec<(usize, FieldSource)>,
}

pub struct Hdf5Appender {
    file: File,
    tables: HashMap<String, TableInfo>,
    pending: HashMap<StreamKey, Vec<SampleBatch>>,
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
            tables: HashMap::new(),
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

    pub fn write_batch(&mut self, batch: SampleBatch, key: StreamKey) -> Result<()> {
        let should_split = !batch.is_initial()
            && match self.split_policy {
                SplitPolicy::Continuous => !batch.is_continuous(),
                SplitPolicy::Monotonic => !batch.is_monotonic(),
            };

        if should_split {
            self.handle_discontinuity(&key)?;
        }

        let chunks = self.pending.entry(key.clone()).or_default();
        chunks.push(batch);

        let rows: usize = chunks.iter().map(|b| b.len()).sum();
        if rows >= self.batch_size {
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
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.pending.keys().cloned().collect();
        for key in keys {
            self.flush_stream(&key)?;
        }
        Ok(())
    }

    /// Name of the compound table for this stream's current run.
    fn table_name(&self, stream_name: &str, key: &StreamKey) -> String {
        match self.split_level {
            RunSplitLevel::None => stream_name.to_string(),
            _ => {
                let run = self.get_run_id(key).unwrap_or(0);
                format!("{}_run{:06}", stream_name, run)
            }
        }
    }

    fn flush_stream(&mut self, key: &StreamKey) -> Result<()> {
        match self.pending.remove(key) {
            Some(chunks) if !chunks.is_empty() => self.write_chunks(key, &chunks),
            _ => Ok(()),
        }
    }

    pub fn finish(mut self) -> Result<ExportStats> {
        let keys: Vec<_> = self.pending.keys().cloned().collect();
        for key in keys {
            self.flush_stream(&key)?;
        }
        Ok(self.stats)
    }

    fn write_chunks(&mut self, key: &StreamKey, chunks: &[SampleBatch]) -> Result<()> {
        let Some(first) = chunks.first() else {
            return Ok(());
        };

        let route_str = key.route.to_string().trim_start_matches('/').to_string();
        let stream_name = first.stream.name.clone();

        // Stream identity for stats counts a stream once, regardless of runs.
        let stream_id_path = if route_str.is_empty() {
            format!("/{}", stream_name)
        } else {
            format!("/{}/{}", route_str, stream_name)
        };
        self.stats.streams_seen.insert(stream_id_path.clone());

        // Apply the column filter; the parser already emits columns index-ordered.
        let mut valid: Vec<&Series> = Vec::new();
        for col in &first.columns {
            if let Some(f) = &self.filter {
                let path = f.get_path_string(&key.route, &stream_name, &col.metadata.name);
                if self.debug && self.seen_debug.insert(path.clone()) {
                    println!(
                        "[DEBUG] Filter: '{}' -> {}",
                        path,
                        f.matches(&key.route, &stream_name, &col.metadata.name)
                    );
                }
                if !f.matches(&key.route, &stream_name, &col.metadata.name) {
                    continue;
                }
            }
            valid.push(col);
        }

        if valid.is_empty() {
            return Ok(());
        }
        valid.sort_by_key(|c| c.index);

        let group_path = if route_str.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", route_str)
        };
        let table_name = self.table_name(&stream_name, key);
        let table_path = if group_path == "/" {
            format!("/{}", table_name)
        } else {
            format!("{}/{}", group_path, table_name)
        };

        // Create the compound table on first sight of this stream/run.
        if !self.tables.contains_key(&table_path) {
            self.ensure_group(&group_path)?;

            let mut fields = Vec::with_capacity(valid.len() + 2);
            fields.push(CompoundField::new(
                "sample",
                TypeDescriptor::Unsigned(IntSize::U4),
                0,
                0,
            ));
            fields.push(CompoundField::new(
                "time",
                TypeDescriptor::Float(FloatSize::U8),
                0,
                1,
            ));
            for (i, col) in valid.iter().enumerate() {
                let ty = match col.values {
                    ColumnVec::F64(_) => TypeDescriptor::Float(FloatSize::U8),
                    ColumnVec::I64(_) => TypeDescriptor::Integer(IntSize::U8),
                    ColumnVec::U64(_) => TypeDescriptor::Unsigned(IntSize::U8),
                };
                fields.push(CompoundField::new(&col.metadata.name, ty, 0, i + 2));
            }

            // `to_c_repr` assigns aligned byte offsets and the total row size.
            let layout = CompoundType { fields, size: 0 }.to_c_repr();
            let desc = TypeDescriptor::Compound(layout.clone());

            let group = self.file.group(&group_path)?;
            // Aim for ~64 KiB chunks: large enough to compress and stream well,
            // small enough that short captures don't over-allocate a chunk tail.
            let chunk_rows = (65_536 / layout.size.max(1)).clamp(256, 65_536);
            let builder = group
                .new_dataset_builder()
                .empty_as(&desc)
                .chunk((chunk_rows,))
                .shape(SimpleExtents::resizable([0usize]));
            let builder = if self.compress {
                builder.blosc(Blosc::BloscLZ, 5, BloscShuffle::Byte)
            } else {
                builder
            };
            let ds = builder.create(table_name.as_str())?;

            self.write_metadata_attributes(&ds, chunks.last().unwrap(), key)?;
            self.write_field_metadata(&ds, &valid)?;

            // Map each (already index-ordered) compound field to its data source.
            let field_sources = layout
                .fields
                .iter()
                .map(|f| {
                    let source = match f.index {
                        0 => FieldSource::Sample,
                        1 => FieldSource::Time,
                        k => FieldSource::Column(valid[k - 2].index),
                    };
                    (f.offset, source)
                })
                .collect();

            self.tables.insert(
                table_path.clone(),
                TableInfo {
                    dataset: ds,
                    row_size: layout.size,
                    fields: field_sources,
                },
            );
        }

        let n: usize = chunks.iter().map(|c| c.len()).sum();
        {
            let info = self.tables.get(&table_path).unwrap();
            let row_size = info.row_size;
            let mut buf = vec![0u8; n * row_size];

            for (offset, source) in &info.fields {
                let offset = *offset;
                let mut row = 0;
                for chunk in chunks {
                    let clen = chunk.len();
                    match source {
                        FieldSource::Sample => {
                            for i in 0..clen {
                                let bytes = chunk.sample_numbers[i].to_ne_bytes();
                                let base = (row + i) * row_size + offset;
                                buf[base..base + bytes.len()].copy_from_slice(&bytes);
                            }
                        }
                        FieldSource::Time => {
                            for i in 0..clen {
                                let bytes = chunk
                                    .segment
                                    .time_at(chunk.sample_numbers[i] + 1)
                                    .to_ne_bytes();
                                let base = (row + i) * row_size + offset;
                                buf[base..base + 8].copy_from_slice(&bytes);
                            }
                        }
                        FieldSource::Column(col_id) => {
                            if let Some(col) = chunk.columns.iter().find(|c| c.index == *col_id) {
                                for i in 0..clen {
                                    let bytes = match &col.values {
                                        ColumnVec::F64(v) => v[i].to_ne_bytes(),
                                        ColumnVec::I64(v) => v[i].to_ne_bytes(),
                                        ColumnVec::U64(v) => v[i].to_ne_bytes(),
                                    };
                                    let base = (row + i) * row_size + offset;
                                    buf[base..base + 8].copy_from_slice(&bytes);
                                }
                            }
                        }
                    }
                    row += clen;
                }
            }

            Self::append_rows(&info.dataset, &buf, n)?;
        }

        self.stats.total_samples += n as u64;
        let last = chunks.last().unwrap();
        if let (Some(first_n), Some(last_n)) = (first.first_sample(), last.last_sample()) {
            let first_t = first.segment.time_at(first_n + 1);
            let last_t = last.segment.time_at(last_n + 1);
            self.stats.start_time = Some(self.stats.start_time.map_or(first_t, |t| t.min(first_t)));
            self.stats.end_time = Some(self.stats.end_time.map_or(last_t, |t| t.max(last_t)));
        }
        self.stats.streams_written.insert(stream_id_path);

        Ok(())
    }

    /// Appends `n` packed compound rows to the end of an extendible table.
    fn append_rows(ds: &Dataset, buf: &[u8], n: usize) -> Result<()> {
        let old = ds.shape()[0];
        ds.resize((old + n,))?;

        let fspace = ds.space()?.select(old..old + n)?;
        let mspace = Dataspace::try_new(SimpleExtents::from([n]))?;
        let memtype = ds.dtype()?;

        // The dataset's own datatype is reused for memory, so the packed,
        // native-endian buffer is written without any conversion.
        let status = unsafe {
            H5Dwrite(
                ds.id(),
                memtype.id(),
                mspace.id(),
                fspace.id(),
                H5P_DEFAULT,
                buf.as_ptr() as *const std::ffi::c_void,
            )
        };
        if status < 0 {
            return Err("H5Dwrite failed while appending compound table rows".into());
        }
        Ok(())
    }

    fn write_metadata_attributes(
        &self,
        loc: &Location,
        batch: &SampleBatch,
        key: &StreamKey,
    ) -> Result<()> {
        let meta = &batch.segment;
        self.write_attr_scalar(loc, "sampling_rate", &meta.sampling_rate)?;
        self.write_attr_scalar(loc, "decimation", &meta.decimation)?;
        self.write_attr_scalar(loc, "start_time", &meta.start_time)?;
        self.write_attr_scalar(loc, "filter_cutoff", &meta.filter_cutoff)?;
        self.write_attr_scalar(loc, "session_id", &batch.device.session_id)?;

        let run_id = self.get_run_id(key);
        if let Some(id) = run_id {
            self.write_attr_scalar(loc, "run_id", &id)?;
        }

        let epoch_u8: u8 = meta.time_ref_epoch.clone().into();
        self.write_attr_scalar(loc, "time_ref_epoch", &epoch_u8)?;

        let filter_type_u8: u8 = meta.filter_type.clone().into();
        self.write_attr_scalar(loc, "filter_type", &filter_type_u8)?;

        if !meta.time_ref_serial.is_empty() {
            self.write_attr_string(loc, "time_ref_serial", &meta.time_ref_serial)?;
        }
        Ok(())
    }

    /// Stores per-field `units` and `descriptions` as string arrays aligned with
    /// the compound fields (`sample`, `time`, then each column).
    fn write_field_metadata(&self, loc: &Location, valid: &[&Series]) -> Result<()> {
        let mut units: Vec<VarLenUnicode> = Vec::with_capacity(valid.len() + 2);
        let mut descriptions: Vec<VarLenUnicode> = Vec::with_capacity(valid.len() + 2);

        units.push(to_vlu(""));
        descriptions.push(to_vlu("Sample number from device"));
        units.push(to_vlu("s"));
        descriptions.push(to_vlu("Time in seconds"));
        for col in valid {
            units.push(to_vlu(&col.metadata.units));
            descriptions.push(to_vlu(&col.metadata.description));
        }

        self.write_attr_string_array(loc, "units", &units)?;
        self.write_attr_string_array(loc, "descriptions", &descriptions)?;
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
        attr.write_scalar(&to_vlu(val))
    }

    fn write_attr_string_array(
        &self,
        loc: &Location,
        name: &str,
        vals: &[VarLenUnicode],
    ) -> Result<()> {
        let attr = loc
            .new_attr::<VarLenUnicode>()
            .shape(SimpleExtents::from([vals.len()]))
            .create(name)?;
        attr.write_raw(vals)
    }
}

fn to_vlu(s: &str) -> VarLenUnicode {
    s.parse::<VarLenUnicode>()
        .unwrap_or_else(|_| "".parse().unwrap())
}

//! HDF5 export of decoded sample batches.
//!
//! [`Hdf5Appender`] writes batches into one file, one group per device route
//! and one compound table per stream:
//!
//! ```text
//! recording.h5
//! ├── vector          root device
//! ├── 0/
//! │   └── accel       device at route /0
//! └── 1/
//!     └── therm
//! ```
//!
//! Every table carries the same leading fields:
//!
//! | Field         | Type             | Source                      |
//! |---------------|------------------|-----------------------------|
//! | `sample`      | u32              | sample number               |
//! | `time`        | f64              | end-of-sample timestamp     |
//! | *column name* | f64, i64, or u64 | one field per stream column |
//!
//! Tables are jagged. Streams sample independently, so two tables in one file
//! do not start or end at the same `time`, even at the same run number. Align
//! rows on the `time` field, never on row index.
//!
//! Two independent choices control how runs land in the file. [`SplitPolicy`]
//! decides which breaks start a new run at all; [`RunSplitLevel`] decides which
//! streams roll to a new table when one does.

use super::filter::ColumnFilter;
use super::metadata::{ColumnRecord, DeviceRecord, StreamRecord};
use super::sample::{BoundaryReason, ColumnArray, Generations, SampleBatch, Series, StreamKey};
use crate::tio::proto::DeviceRoute;
use hdf5::filters::{Blosc, BloscShuffle};
use hdf5::types::{CompoundField, CompoundType, FloatSize, IntSize, TypeDescriptor, VarLenUnicode};
use hdf5::{Dataset, Dataspace, File, H5Type, Location, Result, SimpleExtents};
use hdf5_sys::h5d::H5Dwrite;
use hdf5_sys::h5p::H5P_DEFAULT;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use twinleaf_proto::{ColumnId, SampleNumber};

type TableIndex = u64;

/// Which discontinuities start a new run.
///
/// | Break                                | `Continuous`  | `Monotonic`   |
/// |--------------------------------------|---------------|---------------|
/// | Segment rollover                     | append        | append        |
/// | Samples lost, rate changed           | **new table** | append        |
/// | Session changed, time went backward  | **new table** | **new table** |
///
/// `Continuous` yields tables whose rows are contiguous, one per parser run
/// ([`Generations::stream`]). `Monotonic` yields fewer tables, each possibly
/// spanning several runs, which may contain gaps but never run backward.
#[derive(Debug, Clone, Copy, Default)]
pub enum SplitPolicy {
    /// Split on any discontinuity (default)
    #[default]
    Continuous,
    /// Only split on non-monotonic breaks (allows gaps)
    Monotonic,
}

/// Which streams roll to a new table when a run splits.
///
/// Each run becomes its own table in the same route group. Given one recording
/// where `accel` loses samples midway and `gyro` does not:
///
/// ```text
/// None          PerStream            PerDevice
/// /             /                    /
/// ├── accel     ├── accel_run000000  ├── accel_run000000
/// └── gyro      ├── accel_run000001  ├── accel_run000001
///               └── gyro_run000000   ├── gyro_run000000
///                                    └── gyro_run000001
/// ```
///
/// `PerDevice` rolls every stream on the device at once, so tables sharing a
/// run number cover the same acquisition run and can be read as one block.
/// `Global` does the same across every device in the file.
///
/// Choose by what has to be read together: `PerStream` isolates each stream's
/// own breaks, `PerDevice` and `Global` keep peers aligned across a break so a
/// run number selects one comparable block of streams.
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

/// Scope over which streams share a table counter, per [`RunSplitLevel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RunScope {
    Stream(StreamKey),
    Device(DeviceRoute),
    Global,
}

/// Table numbering driven by the parser's continuity generations.
///
/// The first batch stamped with a new generation carries the boundary that opened
/// it, so it decides once whether the generation gets its own table or aliases the
/// current one; later batches in scope, from any stream, read the index back.
struct RunTables {
    policy: SplitPolicy,
    level: RunSplitLevel,
    /// `(generation in use, table index)` per scope.
    open: HashMap<RunScope, (u32, TableIndex)>,
}

impl RunTables {
    fn new(policy: SplitPolicy, level: RunSplitLevel) -> Self {
        Self {
            policy,
            level,
            open: HashMap::new(),
        }
    }

    fn scope(&self, key: StreamKey) -> Option<RunScope> {
        match self.level {
            RunSplitLevel::None => None,
            RunSplitLevel::PerStream => Some(RunScope::Stream(key)),
            RunSplitLevel::PerDevice => Some(RunScope::Device(key.route)),
            RunSplitLevel::Global => Some(RunScope::Global),
        }
    }

    /// Account for one batch, returning whether its boundary split the run.
    fn observe(
        &mut self,
        key: StreamKey,
        generations: Generations,
        boundary: Option<&BoundaryReason>,
    ) -> bool {
        let splits = boundary.is_some_and(|b| {
            !b.is_initial()
                && match self.policy {
                    SplitPolicy::Continuous => !b.is_continuous(),
                    SplitPolicy::Monotonic => !b.is_monotonic(),
                }
        });
        if let Some(scope) = self.scope(key) {
            let generation = match self.level {
                RunSplitLevel::PerDevice => generations.device,
                RunSplitLevel::Global => generations.global,
                RunSplitLevel::PerStream | RunSplitLevel::None => generations.stream,
            };
            let open = self.open.entry(scope).or_insert((generation, 0));
            if open.0 != generation {
                *open = (generation, open.1 + splits as TableIndex);
            }
        }
        splits
    }

    /// Table index for a stream's current run, or `None` without splitting.
    fn index(&self, key: StreamKey) -> Option<TableIndex> {
        Some(self.open.get(&self.scope(key)?).map_or(0, |open| open.1))
    }
}

/// What one export wrote, returned by [`Hdf5Appender::finish`].
#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    /// Rows written across every table.
    pub total_samples: u64,
    /// Earliest `time` written, or `None` if nothing was.
    pub start_time: Option<f64>,
    /// Latest `time` written, or `None` if nothing was.
    pub end_time: Option<f64>,
    /// Streams that reached the file, as `/{route}/{stream}[{id}]`.
    pub streams_written: HashSet<String>,
    /// Streams seen in the input, including any the filter dropped.
    pub streams_seen: HashSet<String>,
    /// Breaks that [`SplitPolicy`] counted as a split.
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
    schema: TableSchema,
}

/// Structural schema represented by one HDF5 compound table. Segment and
/// session metadata are deliberately excluded so flat exports can span normal
/// rollovers; a different device or column schema must not reuse the layout.
struct TableSchema {
    key: StreamKey,
    device: DeviceRecord,
    stream: StreamRecord,
    columns: Vec<ColumnRecord>,
}

impl TableSchema {
    fn from_batch(batch: &SampleBatch, columns: &[&Series]) -> Self {
        let (device, stream, _) = batch.records();
        Self {
            key: batch.stream_key(),
            device: device.clone(),
            stream: stream.clone(),
            columns: columns
                .iter()
                .map(|column| column.record().clone())
                .collect(),
        }
    }

    fn matches(&self, batch: &SampleBatch, columns: &[&Series]) -> bool {
        self.key == batch.stream_key()
            && self.device.get().serial == batch.device().serial
            && self.stream.get().name == batch.stream().name
            && self.stream.get().sample_size == batch.stream().sample_size
            && self.columns.len() == columns.len()
            && self
                .columns
                .iter()
                .zip(columns)
                .all(|(expected, actual)| *expected == *actual.record())
    }
}

/// Writes [`SampleBatch`]es to one HDF5 file, one table per stream.
///
/// Build with [`Hdf5Appender::with_options`], feed with
/// [`Hdf5Appender::write_batch`], close with [`Hdf5Appender::finish`].
pub struct Hdf5Appender {
    file: File,
    tables: HashMap<String, TableInfo>,
    filter: Option<ColumnFilter>,
    compress: bool,
    debug: bool,
    runs: RunTables,
    seen_debug: HashSet<String>,
    stats: ExportStats,
}

impl Hdf5Appender {
    /// Creates a new appender, failing if `path` already exists.
    pub fn with_options(
        path: &Path,
        compress: bool,
        debug: bool,
        filter: Option<ColumnFilter>,
        split_policy: SplitPolicy,
        split_level: RunSplitLevel,
    ) -> Result<Self> {
        Self::from_file(
            File::create_excl(path)?,
            compress,
            debug,
            filter,
            split_policy,
            split_level,
        )
    }

    fn from_file(
        file: File,
        compress: bool,
        debug: bool,
        filter: Option<ColumnFilter>,
        split_policy: SplitPolicy,
        split_level: RunSplitLevel,
    ) -> Result<Self> {
        Ok(Self {
            file,
            tables: HashMap::new(),
            filter,
            compress,
            debug,
            runs: RunTables::new(split_policy, split_level),
            seen_debug: HashSet::new(),
            stats: ExportStats::default(),
        })
    }

    /// Append an already-decoded batch.
    pub fn write_batch(&mut self, batch: SampleBatch) -> Result<()> {
        let key = batch.stream_key();
        if self
            .runs
            .observe(key, batch.generations(), batch.boundary())
        {
            self.stats.discontinuities_detected += 1;
        }
        if self.debug {
            if let Some(boundary) = batch.boundary() {
                log::info!(
                    "[{}] sample_n={} boundary={:?}",
                    batch.stream().name,
                    batch.first_sample().unwrap_or(SampleNumber::new(0)),
                    boundary
                );
            }
        }
        self.append_batch(&batch)
    }

    /// Flush every open table and close the file.
    pub fn finish(self) -> Result<ExportStats> {
        Ok(self.stats)
    }

    fn append_batch(&mut self, batch: &SampleBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let key = batch.stream_key();
        let route_str = key.route.to_string().trim_start_matches('/').to_string();
        let stream_name = batch.stream().name.to_string();

        // Stream identity for stats counts a stream once, regardless of runs.
        let stream_id_path = if route_str.is_empty() {
            format!("/{}[{}]", stream_name, key.stream_id)
        } else {
            format!("/{}/{}[{}]", route_str, stream_name, key.stream_id)
        };
        self.stats.streams_seen.insert(stream_id_path.clone());

        // Apply the column filter; the parser already emits columns index-ordered.
        let mut valid: Vec<&Series> = Vec::new();
        for col in batch.schema() {
            if let Some(f) = &self.filter {
                let path = f.get_path_string(&key.route, &stream_name, col.metadata().name);
                if self.debug && self.seen_debug.insert(path.clone()) {
                    println!(
                        "[DEBUG] Filter: '{}' -> {}",
                        path,
                        f.matches(&key.route, &stream_name, col.metadata().name)
                    );
                }
                if !f.matches(&key.route, &stream_name, col.metadata().name) {
                    continue;
                }
            }
            valid.push(col);
        }

        if valid.is_empty() {
            return Ok(());
        }
        valid.sort_by_key(|c| c.index());

        let group_path = if route_str.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", route_str)
        };
        // Each run of a stream is its own table in the route group.
        let table_stem = hdf_name_component(&stream_name);
        let table_name = match self.runs.index(key) {
            Some(run) => format!("{table_stem}_run{run:06}"),
            None => table_stem,
        };
        let table_path = if group_path == "/" {
            format!("/{}", table_name)
        } else {
            format!("{}/{}", group_path, table_name)
        };

        if let Some(info) = self.tables.get(&table_path) {
            if !info.schema.matches(batch, &valid) {
                return Err(format!(
                    "metadata or schema changed while writing HDF5 table {table_path}; split the export into distinct runs"
                )
                .into());
            }
        }

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
                let ty = match col.values() {
                    ColumnArray::F64(_) => TypeDescriptor::Float(FloatSize::U8),
                    ColumnArray::I64(_) => TypeDescriptor::Integer(IntSize::U8),
                    ColumnArray::U64(_) => TypeDescriptor::Unsigned(IntSize::U8),
                };
                fields.push(CompoundField::new(col.metadata().name, ty, 0, i + 2));
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

            self.write_metadata_attributes(&ds, batch, &key)?;
            self.write_field_metadata(&ds, &valid)?;

            // Map each (already index-ordered) compound field to its data source.
            let field_sources = layout
                .fields
                .iter()
                .map(|f| {
                    let source = match f.index {
                        0 => FieldSource::Sample,
                        1 => FieldSource::Time,
                        k => FieldSource::Column(valid[k - 2].index()),
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
                    schema: TableSchema::from_batch(batch, &valid),
                },
            );
        }

        let n = batch.len();
        {
            let info = self.tables.get(&table_path).unwrap();
            let row_size = info.row_size;
            let mut buf = vec![0u8; n * row_size];

            for (offset, source) in &info.fields {
                let offset = *offset;
                match source {
                    FieldSource::Sample => {
                        for i in 0..n {
                            let bytes = batch.sample_numbers()[i].value().to_ne_bytes();
                            let base = i * row_size + offset;
                            buf[base..base + bytes.len()].copy_from_slice(&bytes);
                        }
                    }
                    FieldSource::Time => {
                        for i in 0..n {
                            let bytes = batch.timestamps()[i].to_ne_bytes();
                            let base = i * row_size + offset;
                            buf[base..base + 8].copy_from_slice(&bytes);
                        }
                    }
                    FieldSource::Column(col_id) => {
                        if let Some(col) = batch.schema().iter().find(|c| c.index() == *col_id) {
                            for i in 0..n {
                                let bytes = match col.values() {
                                    ColumnArray::F64(v) => v[i].to_ne_bytes(),
                                    ColumnArray::I64(v) => v[i].to_ne_bytes(),
                                    ColumnArray::U64(v) => v[i].to_ne_bytes(),
                                };
                                let base = i * row_size + offset;
                                buf[base..base + 8].copy_from_slice(&bytes);
                            }
                        }
                    }
                }
            }

            Self::append_rows(&info.dataset, &buf, n)?;
        }

        self.stats.total_samples += n as u64;
        if let (Some(&first_t), Some(&last_t)) =
            (batch.timestamps().first(), batch.timestamps().last())
        {
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
        let meta = batch.segment();
        self.write_attr_scalar(loc, "sampling_rate", &meta.sampling_rate)?;
        self.write_attr_scalar(loc, "decimation", &meta.decimation)?;
        self.write_attr_scalar(loc, "start_time", &meta.start_time)?;
        self.write_attr_scalar(loc, "filter_cutoff", &meta.filter_cutoff)?;
        self.write_attr_scalar(loc, "session_id", &batch.device().session.value())?;
        self.write_attr_scalar(loc, "stream_id", &batch.stream().stream_id.value())?;
        self.write_attr_string(loc, "stream_name", batch.stream().name)?;
        self.write_attr_string(loc, "device_serial", batch.device().serial)?;
        self.write_attr_string(loc, "firmware_hash", batch.device().firmware)?;

        if let Some(id) = self.runs.index(*key) {
            self.write_attr_scalar(loc, "run_id", &id)?;
        }

        self.write_attr_scalar(loc, "time_ref_epoch", &meta.epoch.value())?;
        self.write_attr_scalar(loc, "filter_type", &meta.filter_type.value())?;

        if !meta.timeref_serial.is_empty() {
            self.write_attr_string(loc, "time_ref_serial", meta.timeref_serial)?;
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
            units.push(to_vlu(col.metadata().units));
            descriptions.push(to_vlu(col.metadata().description));
        }

        self.write_attr_string_array(loc, "units", &units)?;
        self.write_attr_string_array(loc, "descriptions", &descriptions)?;
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

/// HDF5 treats `/` as a path separator and NUL is not a valid link character.
/// Keep the human-readable stream name while making it a single path component.
fn hdf_name_component(name: &str) -> String {
    let escaped: String = name
        .chars()
        .map(|character| match character {
            '/' | '\0' => '_',
            other => other,
        })
        .collect();
    if escaped.is_empty() {
        "unnamed".to_string()
    } else {
        escaped
    }
}

fn to_vlu(s: &str) -> VarLenUnicode {
    s.parse::<VarLenUnicode>()
        .unwrap_or_else(|_| "".parse().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::metadata::{buffer_type, DeviceRecord, SegmentRecord, StreamRecord};
    use crate::data::sample::{BatchContext, BoundaryReason, ColumnData, SampleBatchBuilder};
    use crate::tio::proto::DataType;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard};
    use twinleaf_proto::data as wire;

    fn key(stream_id: u8) -> StreamKey {
        StreamKey::new(
            DeviceRoute::root(),
            twinleaf_proto::StreamId::new(stream_id),
        )
    }

    /// Generations as the parser stamps them for a stream whose own run is `stream`:
    /// only the boundaries after the initial one bump the shared generations.
    fn generations(stream: u32) -> Generations {
        Generations {
            stream,
            device: stream - 1,
            global: stream - 1,
        }
    }

    fn lost() -> BoundaryReason {
        BoundaryReason::SamplesLost {
            expected: SampleNumber::new(2),
            received: SampleNumber::new(9),
        }
    }

    fn batch(stream_id: u8, stream_name: &str, column_name: &str) -> SampleBatch {
        batch_in_segment(stream_id, stream_name, column_name, 0)
    }

    fn batch_in_segment(
        stream_id: u8,
        stream_name: &str,
        column_name: &str,
        start_time: u32,
    ) -> SampleBatch {
        let column = ColumnRecord::encode(wire::Column {
            name: column_name,
            units: "V",
            description: "test column",
            ..fixtures::column(stream_id, 0, DataType::F32)
        })
        .unwrap();
        let mut builder = SampleBatchBuilder::new(
            BatchContext::new(
                key(stream_id),
                None,
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0,
                },
                SegmentRecord::encode(wire::Segment {
                    segment_id: twinleaf_proto::SegmentId::new(1),
                    start_time,
                    ..fixtures::segment(stream_id)
                })
                .unwrap(),
                StreamRecord::encode(wire::Stream {
                    name: stream_name,
                    buf_samples: 1,
                    ..fixtures::stream(stream_id)
                })
                .unwrap(),
                DeviceRecord::encode(wire::Device {
                    n_streams: 2,
                    ..fixtures::device()
                })
                .unwrap(),
            ),
            [(column.clone(), buffer_type(column.get().data_type))],
            1,
        );
        builder.push_row(SampleNumber::new(0), [ColumnData::Float(1.0)]);
        builder.finish()
    }

    fn temp_hdf(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "twinleaf_export_{label}_{}_{}.h5",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn appender(path: &Path) -> Hdf5Appender {
        Hdf5Appender::with_options(
            path,
            false,
            false,
            None,
            SplitPolicy::Continuous,
            RunSplitLevel::None,
        )
        .expect("create test HDF5 output")
    }

    fn hdf_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().expect("lock HDF5 tests")
    }

    #[test]
    fn only_qualifying_boundaries_advance_the_table_index() {
        let opened = [
            BoundaryReason::Initial,
            lost(),
            BoundaryReason::SessionChanged {
                old: twinleaf_proto::SessionId::new(1),
                new: twinleaf_proto::SessionId::new(2),
            },
        ];
        // Data loss is still monotonic, so only the session change splits there.
        for (policy, expected) in [
            (SplitPolicy::Continuous, [0, 1, 2]),
            (SplitPolicy::Monotonic, [0, 0, 1]),
        ] {
            let mut runs = RunTables::new(policy, RunSplitLevel::PerStream);
            let indices: Vec<TableIndex> = opened
                .iter()
                .enumerate()
                .map(|(run, boundary)| {
                    runs.observe(key(1), generations(run as u32 + 1), Some(boundary));
                    runs.index(key(1)).expect("split tables are numbered")
                })
                .collect();
            assert_eq!(indices, expected, "{policy:?}");
        }
    }

    #[test]
    fn a_device_generation_moves_every_stream_of_the_device_to_one_table() {
        let mut runs = RunTables::new(SplitPolicy::Continuous, RunSplitLevel::PerDevice);
        runs.observe(key(1), generations(1), Some(&BoundaryReason::Initial));
        runs.observe(key(1), generations(2), Some(&lost()));
        assert_eq!(runs.index(key(1)), Some(1));
        // A stream that never saw the boundary reads the generation's table back.
        assert_eq!(runs.index(key(2)), Some(1));

        // Rows still stamped with the old generation land in the table in use.
        assert!(!runs.observe(key(2), generations(1), None));
        assert_eq!(runs.index(key(2)), Some(1));
    }

    #[test]
    fn unsplit_tables_have_no_run_index() {
        let mut runs = RunTables::new(SplitPolicy::Continuous, RunSplitLevel::None);
        // The discontinuity is still reported, it just does not open a table.
        assert!(runs.observe(key(1), generations(2), Some(&lost())));
        assert_eq!(runs.index(key(1)), None);
    }

    #[test]
    fn safe_constructor_does_not_replace_existing_files() {
        let _guard = hdf_test_lock();
        let path = temp_hdf("exclusive");
        std::fs::write(&path, b"existing input").expect("create sentinel");

        assert!(Hdf5Appender::with_options(
            &path,
            false,
            false,
            None,
            SplitPolicy::Continuous,
            RunSplitLevel::None,
        )
        .is_err());
        assert_eq!(
            std::fs::read(&path).expect("read sentinel"),
            b"existing input"
        );

        std::fs::remove_file(path).expect("remove sentinel");
    }

    #[test]
    fn duplicate_stream_names_are_rejected_by_the_schema_guard() {
        let _guard = hdf_test_lock();
        let path = temp_hdf("duplicate_names");
        let mut writer = appender(&path);
        writer
            .write_batch(batch(1, "field", "x"))
            .expect("write first stream");
        let error = writer
            .write_batch(batch(2, "field", "x"))
            .expect_err("a second stream of the same name must fail");
        assert!(error.to_string().contains("schema changed"));
        drop(writer);
        std::fs::remove_file(path).expect("remove output");
    }

    #[test]
    fn changed_schema_is_rejected_instead_of_reinterpreted() {
        let _guard = hdf_test_lock();
        let path = temp_hdf("schema_change");
        let mut writer = appender(&path);
        writer
            .write_batch(batch(1, "field", "x"))
            .expect("write original schema");
        let error = writer
            .write_batch(batch(1, "field", "renamed"))
            .expect_err("schema change must fail");
        assert!(error.to_string().contains("schema changed"));
        drop(writer);
        std::fs::remove_file(path).expect("remove output");
    }

    #[test]
    fn normal_segment_rollover_reuses_a_flat_table() {
        let _guard = hdf_test_lock();
        let path = temp_hdf("segment_rollover");
        let mut writer = appender(&path);
        writer
            .write_batch(batch_in_segment(1, "field", "x", 0))
            .expect("write first segment");
        writer
            .write_batch(batch_in_segment(1, "field", "x", 100))
            .expect("write next segment");
        writer.finish().expect("finish output");

        let file = File::open(&path).expect("open output");
        assert_eq!(
            file.dataset("field").expect("open stream table").shape(),
            vec![2]
        );
        drop(file);
        std::fs::remove_file(path).expect("remove output");
    }
}

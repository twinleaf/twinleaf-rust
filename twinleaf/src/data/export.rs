//! HDF5 export of decoded sample batches.
//!
//! [`Hdf5Appender`] writes batches into one file, with one group per device
//! route and one compound table per stream.
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
//! Every table starts with the same fields.
//!
//! | Field         | Type                     | Source                      |
//! |---------------|--------------------------|-----------------------------|
//! | `sample`      | u32                      | sample number               |
//! | `time`        | f64                      | end-of-sample timestamp     |
//! | *column name* | the column's wire type   | one field per stream column |
//!
//! Fields are little-endian at C-aligned offsets. Compression is byte shuffle
//! then deflate, which every HDF5 reader decodes without a plugin.
//!
//! Tables are jagged. Streams sample independently, so two tables in one file
//! do not start or end at the same `time`, even at the same run number. Align
//! rows on the `time` field, not on row index.
//!
//! Two independent choices control how runs land in the file. [`SplitPolicy`]
//! decides which breaks start a new run at all. [`RunSplitLevel`] decides
//! which streams roll to a new table when one does.

use super::continuity::{Boundary, Generations};
use super::filter::ColumnFilter;
use super::metadata::{StreamKey, StreamMetadataSnapshot};
use super::sample::{native_types, ColumnArray, SampleBatch};
use crate::proto::data as wire;
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, SampleNumber};
use hdf5_pure::{
    AttrValue, CompoundTypeBuilder, Dataset, File, FileAccessProperties, FileCreateProperties,
    Group, MaxExtent, SyncPolicy,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

type TableIndex = u64;

/// The deflate level of a compressed table, zlib's balance of speed and size.
const DEFLATE_LEVEL: u32 = 4;

/// An error from writing an HDF5 export.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExportError {
    /// The output file could not be created, or already exists.
    #[error("cannot create {}: {source}", path.display())]
    Create {
        /// The path asked for.
        path: PathBuf,
        /// Why it could not be created.
        source: std::io::Error,
    },
    /// A batch's device or column schema differs from that of its table.
    #[error("metadata or schema changed while writing HDF5 table {table}; split the export into distinct runs")]
    SchemaChanged {
        /// The table's path in the file.
        table: String,
    },
    /// The HDF5 writer failed.
    #[error(transparent)]
    Hdf5(#[from] hdf5_pure::Error),
}

/// The result of an export operation.
pub type Result<T> = std::result::Result<T, ExportError>;

/// Which discontinuities start a new run.
///
/// | Break                                | `Continuous`  | `Monotonic`   |
/// |--------------------------------------|---------------|---------------|
/// | Segment rollover                     | append        | append        |
/// | Samples lost, rate changed           | **new table** | append        |
/// | Session changed, time went backward  | **new table** | **new table** |
/// | Stream started over                  | **new table** | **new table** |
///
/// `Continuous` yields tables whose rows are contiguous, one per parser run
/// ([`Generations::stream`]). `Monotonic` yields fewer tables, each possibly
/// spanning several runs, which may contain gaps but always run forward in
/// time.
#[derive(Debug, Clone, Copy, Default)]
pub enum SplitPolicy {
    /// Any discontinuity starts a new run. This is the default.
    #[default]
    Continuous,
    /// Only non-monotonic breaks start a new run. Tables may contain gaps.
    Monotonic,
}

/// Which streams roll to a new table when a run splits.
///
/// Each run becomes its own table in the same route group. Given one recording
/// where `accel` loses samples midway and `gyro` does not, each level gives
/// these tables.
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
/// Choose by what has to be read together. `PerStream` isolates each stream's
/// own breaks. `PerDevice` and `Global` keep peers aligned across a break, and
/// a run number selects one comparable block of streams.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunSplitLevel {
    /// No run splitting. Each stream has one table, `/{route}/{stream}`. This
    /// is the default.
    #[default]
    None,
    /// Each stream has its own run counter, with tables named
    /// `/{route}/{stream}_run{id}`.
    PerStream,
    /// All streams on a device share a run counter, with tables named
    /// `/{route}/{stream}_run{id}`.
    PerDevice,
    /// All streams in the file share a run counter, with tables named
    /// `/{route}/{stream}_run{id}`.
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
/// The first batch in scope stamped with a new generation decides whether the
/// generation gets its own table or shares the current one. A boundary the
/// policy splits at gives it one, and so does a
/// [`Change::Initial`](crate::data::Change::Initial), since a
/// stream's first data never moves a generation its peers share. A new
/// generation at an Initial is of a device that started over. Later batches
/// in scope, from any stream, read the index back.
pub(super) struct RunTables {
    policy: SplitPolicy,
    level: RunSplitLevel,
    /// `(generation in use, table index)` per scope.
    open: HashMap<RunScope, (u32, TableIndex)>,
    /// The streams a batch was seen of. A stream seen before that starts
    /// over counts as a split whatever the policy.
    seen: HashSet<StreamKey>,
}

impl RunTables {
    pub(super) fn new(policy: SplitPolicy, level: RunSplitLevel) -> Self {
        Self {
            policy,
            level,
            open: HashMap::new(),
            seen: HashSet::new(),
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

    /// Accounts for one batch and returns `true` if its boundary split the run.
    pub(super) fn observe(
        &mut self,
        key: StreamKey,
        generations: Generations,
        boundary: Option<&Boundary>,
    ) -> bool {
        let seen = !self.seen.insert(key);
        let (splits, advances) = match boundary {
            Some(b) if b.is_initial() => (seen, true),
            Some(b) => {
                let splits = match self.policy {
                    SplitPolicy::Continuous => !b.is_continuous(),
                    SplitPolicy::Monotonic => !b.is_monotonic(),
                };
                (splits, splits)
            }
            None => (false, false),
        };
        if let Some(scope) = self.scope(key) {
            let generation = match self.level {
                RunSplitLevel::PerDevice => generations.device,
                RunSplitLevel::Global => generations.global,
                RunSplitLevel::PerStream | RunSplitLevel::None => generations.stream,
            };
            let open = self.open.entry(scope).or_insert((generation, 0));
            if open.0 != generation {
                *open = (generation, open.1 + advances as TableIndex);
            }
        }
        splits
    }

    /// Returns the table index for a stream's current run, or `None` if runs
    /// do not split.
    pub(super) fn index(&self, key: StreamKey) -> Option<TableIndex> {
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
    pub streams_written: std::collections::HashSet<String>,
    /// Streams seen in the input, including any the filter dropped.
    pub streams_seen: std::collections::HashSet<String>,
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
    /// The metadata of the table's first batch. Later batches must share its
    /// schema and device serial. Segment and session may change, so a table
    /// spans segment rollovers.
    metadata: StreamMetadataSnapshot,
}

impl TableInfo {
    fn accepts(&self, batch: &SampleBatch) -> bool {
        self.metadata.schema() == batch.schema()
            && self.metadata.device().serial() == batch.metadata().device().serial()
    }
}

/// Writes [`SampleBatch`]es to one HDF5 file, one table per stream.
///
/// Create it with [`Hdf5Appender::with_options`], feed it with
/// [`Hdf5Appender::write_batch`], and close it with [`Hdf5Appender::finish`].
pub struct Hdf5Appender {
    file: File,
    /// Every group created so far, by path. `/` is the root.
    groups: HashMap<String, Group>,
    tables: HashMap<String, TableInfo>,
    filter: Option<ColumnFilter>,
    compress: bool,
    runs: RunTables,
    stats: ExportStats,
}

impl std::fmt::Debug for Hdf5Appender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hdf5Appender")
            .field("file", &self.file)
            .field("tables", &self.tables.keys())
            .field("filter", &self.filter)
            .field("compress", &self.compress)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl Hdf5Appender {
    /// Creates an appender that writes a new HDF5 file at `path`.
    ///
    /// `compress` turns on shuffle and deflate compression. `filter` keeps only
    /// the columns it selects. `split_policy` and `split_level` decide how
    /// runs map to tables.
    ///
    /// # Errors
    ///
    /// Returns [`ExportError::Create`] if `path` already exists or cannot be
    /// created, and an HDF5 error if the file cannot be initialized.
    pub fn with_options(
        path: &Path,
        compress: bool,
        filter: Option<ColumnFilter>,
        split_policy: SplitPolicy,
        split_level: RunSplitLevel,
    ) -> Result<Self> {
        // Claims the path first, since the HDF5 writer would replace a file
        // already there.
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| ExportError::Create {
                path: path.to_path_buf(),
                source,
            })?;
        // One sync when the file closes, as libhdf5 does, rather than one per
        // append.
        let file = File::create_with_options(
            path,
            FileCreateProperties::new(),
            FileAccessProperties::new().with_sync_policy(SyncPolicy::OnClose),
        )
        .inspect_err(|_| {
            let _ = std::fs::remove_file(path);
        })?;
        Ok(Self {
            groups: [("/".to_string(), file.root())].into_iter().collect(),
            file,
            tables: HashMap::new(),
            filter,
            compress,
            runs: RunTables::new(split_policy, split_level),
            stats: ExportStats::default(),
        })
    }

    /// Appends `batch` to its stream's table, creating the table on first
    /// sight.
    ///
    /// # Errors
    ///
    /// Returns an error if HDF5 cannot create or extend the table, or if the
    /// batch's device or column schema differs from that of its table.
    pub fn write_batch(&mut self, batch: SampleBatch) -> Result<()> {
        let key = batch.stream_key();
        if self
            .runs
            .observe(key, batch.generations(), batch.boundary())
        {
            self.stats.discontinuities_detected += 1;
        }
        if let Some(boundary) = batch.boundary() {
            log::debug!(
                "[{}] sample_n={} boundary={:?}",
                batch.schema().stream().name(),
                batch
                    .sample_numbers()
                    .first()
                    .copied()
                    .unwrap_or(SampleNumber::new(0)),
                boundary
            );
        }
        self.append_batch(&batch)
    }

    /// Closes the file, flushing every table, and returns what the export
    /// wrote.
    ///
    /// # Errors
    ///
    /// Returns an HDF5 error if the file cannot be committed or synced.
    pub fn finish(self) -> Result<ExportStats> {
        let Self {
            file,
            groups,
            tables,
            stats,
            ..
        } = self;
        // Every table and group handle holds the file's lock, so they go
        // before the file closes.
        drop(tables);
        drop(groups);
        file.close()?;
        Ok(stats)
    }

    fn append_batch(&mut self, batch: &SampleBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let key = batch.stream_key();
        let route_str = key.route.to_string().trim_start_matches('/').to_string();
        let stream_name = batch.schema().stream().name().to_string();

        // Stream identity for stats counts a stream once, regardless of runs.
        let stream_id_path = if route_str.is_empty() {
            format!("/{}[{}]", stream_name, key.stream_id)
        } else {
            format!("/{}/{}[{}]", route_str, stream_name, key.stream_id)
        };
        self.stats.streams_seen.insert(stream_id_path.clone());

        // Columns arrive in index order, so the kept ones stay in it.
        let valid: Vec<wire::ColumnView<'_>> = batch
            .schema()
            .columns()
            .filter(|column| {
                self.filter
                    .as_ref()
                    .is_none_or(|f| f.matches(&key.route, &stream_name, column.name()))
            })
            .collect();

        if valid.is_empty() {
            return Ok(());
        }

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
            if !info.accepts(batch) {
                return Err(ExportError::SchemaChanged { table: table_path });
            }
        }

        // Create the compound table on first sight of this stream/run.
        if !self.tables.contains_key(&table_path) {
            // Fields sit where a C struct of them would put them: each at a
            // multiple of its own size, and the row padded to 8 bytes.
            let mut offsets = Vec::with_capacity(valid.len());
            let mut end: usize = 16;
            for col in &valid {
                let size = col.data_type().size();
                let offset = end.next_multiple_of(size);
                offsets.push(offset);
                end = offset + size;
            }
            let row_size = end.next_multiple_of(8);
            let mut layout = CompoundTypeBuilder::with_size(row_size as u32)
                .u32_field("sample", 0)
                .f64_field("time", 8);
            let mut fields = vec![(0, FieldSource::Sample), (8, FieldSource::Time)];
            for (col, &offset) in valid.iter().zip(&offsets) {
                let values = batch
                    .column(col.index())
                    .expect("one array per schema column");
                let (name, at) = (col.name(), offset as u64);
                layout = match values {
                    ColumnArray::I8(_) => layout.i8_field(name, at),
                    ColumnArray::U8(_) => layout.u8_field(name, at),
                    ColumnArray::I16(_) => layout.i16_field(name, at),
                    ColumnArray::U16(_) => layout.u16_field(name, at),
                    ColumnArray::I32(_) => layout.i32_field(name, at),
                    ColumnArray::U32(_) => layout.u32_field(name, at),
                    ColumnArray::I64(_) => layout.i64_field(name, at),
                    ColumnArray::U64(_) => layout.u64_field(name, at),
                    ColumnArray::F32(_) => layout.f32_field(name, at),
                    ColumnArray::F64(_) => layout.f64_field(name, at),
                };
                fields.push((offset, FieldSource::Column(col.index())));
            }
            let datatype = layout.build().map_err(hdf5_pure::Error::from)?;

            let mut attributes = self.metadata_attributes(batch, &key);
            attributes.extend(field_metadata_attributes(&valid));

            // Aim for ~64 KiB chunks: large enough to compress and stream well,
            // small enough that short captures don't over-allocate a chunk tail.
            let chunk_rows = (65_536 / row_size).clamp(256, 65_536) as u64;
            let compress = self.compress;
            let ds = self
                .ensure_group(&group_path)?
                .create_dataset(&table_name, |builder| {
                    builder
                        .with_compound_data(datatype, Vec::new(), 0)
                        .with_shape(&[0])
                        .with_maxshape(&[MaxExtent::Unlimited])
                        .with_chunks(&[chunk_rows]);
                    if compress {
                        builder.with_shuffle().with_deflate(DEFLATE_LEVEL);
                    }
                    for (name, value) in attributes {
                        builder.set_attr(name, value);
                    }
                })?;
            // Appends need the table in the file, not only staged.
            self.file.commit()?;

            self.tables.insert(
                table_path.clone(),
                TableInfo {
                    dataset: ds,
                    row_size,
                    fields,
                    metadata: batch.metadata().clone(),
                },
            );
        }

        let n = batch.len();
        {
            let info = self.tables.get_mut(&table_path).unwrap();
            let row_size = info.row_size;
            let mut buf = vec![0u8; n * row_size];

            for (offset, source) in &info.fields {
                let offset = *offset;
                match source {
                    FieldSource::Sample => {
                        for i in 0..n {
                            let bytes = batch.sample_numbers()[i].value().to_le_bytes();
                            let base = i * row_size + offset;
                            buf[base..base + bytes.len()].copy_from_slice(&bytes);
                        }
                    }
                    FieldSource::Time => {
                        for i in 0..n {
                            let bytes = batch.timestamps()[i].to_le_bytes();
                            let base = i * row_size + offset;
                            buf[base..base + 8].copy_from_slice(&bytes);
                        }
                    }
                    FieldSource::Column(col_id) => {
                        if let Some(values) = batch.column(*col_id) {
                            write_column(&mut buf, values, row_size, offset);
                        }
                    }
                }
            }

            // Extends the table by `n` rows and writes the packed buffer there.
            info.dataset.append_raw(&buf)?;
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

    /// Returns the attributes describing a table's stream, segment, and run.
    fn metadata_attributes(
        &self,
        batch: &SampleBatch,
        key: &StreamKey,
    ) -> Vec<(&'static str, AttrValue)> {
        let meta = batch.metadata().segment();
        let mut attributes = vec![
            ("sampling_rate", AttrValue::U32(meta.sampling_rate())),
            ("decimation", AttrValue::U32(meta.decimation())),
            ("start_time", AttrValue::U32(meta.start_time())),
            ("filter_cutoff", AttrValue::F32(meta.filter_cutoff())),
            (
                "session_id",
                batch.metadata().device().session().value().attr(),
            ),
            (
                "stream_id",
                batch.schema().stream().stream_id().value().attr(),
            ),
            ("stream_name", string_attr(batch.schema().stream().name())),
            (
                "device_serial",
                string_attr(batch.metadata().device().serial()),
            ),
            (
                "firmware_hash",
                string_attr(batch.metadata().device().firmware()),
            ),
        ];
        if let Some(id) = self.runs.index(*key) {
            attributes.push(("run_id", AttrValue::U64(id)));
        }
        attributes.push(("time_ref_epoch", meta.epoch().value().attr()));
        attributes.push(("filter_type", meta.filter_type().value().attr()));
        if !meta.timeref_serial().is_empty() {
            attributes.push(("time_ref_serial", string_attr(meta.timeref_serial())));
        }
        attributes
    }

    /// Returns the group at `path`, creating it and any missing parents.
    fn ensure_group(&mut self, path: &str) -> Result<&Group> {
        if !self.groups.contains_key(path) {
            let mut current = String::new();
            for part in path.split('/').filter(|s| !s.is_empty()) {
                let parent = if current.is_empty() { "/" } else { &current };
                let parent = self.groups[parent].clone();
                current.push('/');
                current.push_str(part);
                if !self.groups.contains_key(&current) {
                    self.groups
                        .insert(current.clone(), parent.create_group(part)?);
                }
            }
        }
        Ok(&self.groups[path])
    }
}

/// Writes each of `values` little-endian into the field `offset` bytes into
/// its row of `rows`, which packs rows `row_size` bytes apart.
fn write_column(rows: &mut [u8], values: &ColumnArray, row_size: usize, offset: usize) {
    macro_rules! write {
        ($($variant:ident $native:ty),* $(,)?) => {
            match values {
                $(
                    ColumnArray::$variant(values) => {
                        for (row, value) in rows.chunks_exact_mut(row_size).zip(values.iter()) {
                            row[offset..offset + size_of::<$native>()]
                                .copy_from_slice(&value.to_le_bytes());
                        }
                    }
                )*
            }
        };
    }
    native_types!(write)
}

/// Returns the `units` and `descriptions` attributes, string arrays aligned
/// with the compound fields (`sample`, `time`, then each column).
fn field_metadata_attributes(valid: &[wire::ColumnView<'_>]) -> [(&'static str, AttrValue); 2] {
    let mut units = vec![String::new(), "s".to_string()];
    let mut descriptions = vec![
        "Sample number from device".to_string(),
        "Time in seconds".to_string(),
    ];
    for col in valid {
        units.push(col.units().to_string());
        descriptions.push(col.description().to_string());
    }
    [
        ("units", AttrValue::VarLenStringArray(units)),
        ("descriptions", AttrValue::VarLenStringArray(descriptions)),
    ]
}

/// A protocol identifier's value, stored as an attribute of its own width.
trait ScalarAttr {
    fn attr(self) -> AttrValue;
}

impl ScalarAttr for u8 {
    fn attr(self) -> AttrValue {
        AttrValue::U8(self)
    }
}

impl ScalarAttr for u16 {
    fn attr(self) -> AttrValue {
        AttrValue::U16(self)
    }
}

impl ScalarAttr for u32 {
    fn attr(self) -> AttrValue {
        AttrValue::U32(self)
    }
}

fn string_attr(value: &str) -> AttrValue {
    AttrValue::VarLenString(value.to_string())
}

/// Returns `name` as one HDF5 path component, with each `/` and NUL replaced
/// by `_`. An empty name becomes `unnamed`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::continuity::{Change, Loss};
    use crate::data::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::data::tests::fixtures;
    use crate::proto::data::DataType;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard};

    fn key(stream_id: u8) -> StreamKey {
        StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(stream_id))
    }

    /// Generations as the parser stamps them for a stream whose own run is
    /// `stream`. Only the boundaries after the initial one bump the shared
    /// generations.
    fn generations(stream: u32) -> Generations {
        Generations {
            stream,
            device: stream - 1,
            global: stream - 1,
        }
    }

    fn loss() -> Loss {
        Loss {
            samples: 7,
            expected: Some(SampleNumber::new(2)),
            dropped_batches: 0,
        }
    }

    fn lost() -> Boundary {
        Boundary {
            change: None,
            loss: Some(loss()),
        }
    }

    fn initial() -> Boundary {
        Boundary::of(Change::Initial)
    }

    fn batch(stream_id: u8, stream_name: &str, column_name: &str) -> SampleBatch {
        batch_in_segment(DeviceRoute::root(), stream_id, stream_name, column_name, 0)
    }

    fn batch_in_segment(
        route: DeviceRoute,
        stream_id: u8,
        stream_name: &str,
        column_name: &str,
        start_time: u32,
    ) -> SampleBatch {
        let mut builder = SampleBatchBuilder::new(
            BatchContext {
                metadata: fixtures::metadata(
                    route,
                    wire::Device {
                        n_streams: 2,
                        ..fixtures::device()
                    },
                    wire::Stream {
                        name: stream_name,
                        buf_samples: 1,
                        ..fixtures::stream(stream_id)
                    },
                    wire::Segment {
                        segment_id: crate::proto::SegmentId::new(1),
                        start_time,
                        ..fixtures::segment(stream_id)
                    },
                    &[wire::Column {
                        name: column_name,
                        units: "V",
                        description: "test column",
                        ..fixtures::column(stream_id, 0, DataType::F64)
                    }],
                ),
                boundary: None,
                generations: Generations {
                    stream: 1,
                    device: 0,
                    global: 0,
                },
            },
            1,
        );
        builder.push_row(SampleNumber::new(0), [ColumnData::F64(1.0)]);
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
            None,
            SplitPolicy::Continuous,
            RunSplitLevel::None,
        )
        .expect("create test HDF5 output")
    }

    fn hdf_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        crate::chan::lock(&LOCK)
    }

    #[test]
    fn only_qualifying_boundaries_advance_the_table_index() {
        let rolled_over_with_loss = Boundary {
            change: Some(Change::SegmentRollover {
                old_id: crate::proto::SegmentId::new(0),
                new_id: crate::proto::SegmentId::new(1),
            }),
            loss: Some(loss()),
        };
        let opened = [
            initial(),
            lost(),
            rolled_over_with_loss,
            Boundary::of(Change::SessionChanged {
                old: crate::proto::SessionId::new(1),
                new: crate::proto::SessionId::new(2),
            }),
        ];
        // Data loss is still monotonic, with a rollover or without, so only
        // the session change splits there.
        for (policy, expected) in [
            (SplitPolicy::Continuous, [0, 1, 2, 3]),
            (SplitPolicy::Monotonic, [0, 0, 0, 1]),
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

    /// A device that starts over moves to a new table at the device level,
    /// whether the first of its streams to send after is one seen before or
    /// not. A stream's first data at startup moves nothing.
    #[test]
    fn a_device_that_starts_over_moves_every_stream_to_a_new_table() {
        let mut runs = RunTables::new(SplitPolicy::Continuous, RunSplitLevel::PerDevice);
        let stamped = |stream, device| Generations {
            stream,
            device,
            global: device,
        };
        runs.observe(key(1), stamped(1, 0), Some(&initial()));
        runs.observe(key(2), stamped(1, 0), Some(&initial()));
        assert_eq!(runs.index(key(2)), Some(0), "a second stream starting up");
        runs.observe(key(3), stamped(1, 1), Some(&initial()));
        runs.observe(key(1), stamped(2, 1), Some(&initial()));
        assert_eq!(runs.index(key(1)), Some(1), "the device started over");
        assert_eq!(runs.index(key(3)), Some(1));
    }

    #[test]
    fn a_device_generation_moves_every_stream_of_the_device_to_one_table() {
        let mut runs = RunTables::new(SplitPolicy::Continuous, RunSplitLevel::PerDevice);
        runs.observe(key(1), generations(1), Some(&initial()));
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
            .write_batch(batch_in_segment(DeviceRoute::root(), 1, "field", "x", 0))
            .expect("write first segment");
        writer
            .write_batch(batch_in_segment(DeviceRoute::root(), 1, "field", "x", 100))
            .expect("write next segment");
        writer.finish().expect("finish output");

        let file = File::open(&path).expect("open output");
        assert_eq!(
            file.dataset("field")
                .expect("open stream table")
                .shape()
                .expect("read table shape"),
            vec![2]
        );
        drop(file);
        std::fs::remove_file(path).expect("remove output");
    }

    #[test]
    fn each_route_gets_a_group_with_its_parents() {
        let _guard = hdf_test_lock();
        let path = temp_hdf("nested_routes");
        let mut writer = appender(&path);
        // The deeper route comes first, so its parent group is created for it.
        for route in ["/0/2", "/0", "/1"] {
            let route = route.parse().expect("parse route");
            writer
                .write_batch(batch_in_segment(route, 1, "field", "x", 0))
                .expect("write routed stream");
        }
        writer.finish().expect("finish output");

        let file = File::open(&path).expect("open output");
        for table in ["0/2/field", "0/field", "1/field"] {
            assert_eq!(
                file.dataset(table)
                    .expect("open routed table")
                    .shape()
                    .expect("read table shape"),
                vec![1],
                "{table}"
            );
        }
        drop(file);
        std::fs::remove_file(path).expect("remove output");
    }

    mod read_back {
        //! Properties of an export, read back.
        //!
        //! # Observations
        //!
        //! The tables of a finished export, opened with `hdf5-pure`. For each
        //! table, its compound type, the name, offset and type of each field,
        //! the row size, and the bytes of every row.
        //!
        //! # Sources
        //!
        //! The module doc of the export. Its tree puts a stream at route `/0`
        //! in the group `0`. Its table of fields gives `sample` as the sample
        //! number, `time` as the end of sample timestamp and one field per
        //! column of the column's wire type. Fields are little endian at
        //! C aligned offsets. [`RunSplitLevel::None`] gives each stream one
        //! table, `/{route}/{stream}`. [`ExportStats::total_samples`] counts
        //! the rows written across every table.
        //!
        //! # Model
        //!
        //! A stream's table holds the rows of its batches in the order they
        //! were written. Row `i` holds the batch's sample number, its time and
        //! each column's value, read from the [`SampleBatch`] itself.
        //!
        //! # Laws
        //!
        //! - E1, round trip. Every field of every row reads back as the batch
        //!   holds it, bit for bit, in a table at the route's group named for
        //!   the stream, with as many rows as were written. A stream given no
        //!   batch has no table, and one given only empty batches has no rows.
        //!   Source, the table of fields and the tree. It rules out a writer
        //!   that puts every row's sample number or time at one place in the
        //!   buffer, one whose 1 byte column shares its offset with the next
        //!   field, one that keys its tables by stream id or name without the
        //!   route, and one that loses rows past the first chunk of a table.
        //! - E2, postcondition. The fields are `sample`, `time`, then each
        //!   column in index order, each of its own type. Each sits at a
        //!   multiple of its size, after the end of the one before, with less
        //!   padding between them than its size. The row ends at the first
        //!   multiple of 8 after the last field, as a C struct holding an
        //!   `f64` does. Source, the sentence on C aligned offsets. It rules
        //!   out a row that leaves gaps no C compiler leaves, or packs fields
        //!   at offsets a C reader of the struct would not find them at.
        //! - E3, postcondition. The stats count every row written. Source,
        //!   [`ExportStats::total_samples`].
        //!
        //! # Generator obligations
        //!
        //! One to three streams, each a distinct pair of a route of 0 to 2
        //! hops and a stream id of 1 or 2, so one id often sits at two routes
        //! and one route often holds two ids. Each stream has one to five
        //! columns of every native type, with `u8` and `i8` three times as
        //! likely as the others so that 1 byte columns sit next to each other
        //! and next to wider ones, or else 33 to 36 columns, whose rows of
        //! 256 bytes or more fill an HDF5 chunk of the smallest size in 256
        //! rows. Values of arbitrary bits, so floats include NaNs and
        //! infinities. Each stream gets batches of 0 to 24 rows, or of 257 to
        //! 600, interleaved with the others' and with gaps in the sample
        //! numbers between them, so a table spans several appends and, with
        //! the long batches, several chunks. Compression on and off.
        //!
        //! # Exclusions
        //!
        //! The table attributes, which the docs do not list, so whether
        //! `time_ref_serial` is written is not checked. Whether an empty batch
        //! creates its stream's table, which the doc of
        //! [`Hdf5Appender::write_batch`] leaves open, so only its row count is
        //! checked. Run splitting and the column filter, which the run table
        //! tests cover.
        //!
        //! # Examples that stay
        //!
        //! One literal layout, as the export's own C struct of five columns
        //! and one row, since the writer and this reader could agree on a
        //! wrong layout.

        use super::*;
        use crate::data::sample::ColumnBuilder;
        use hdf5_pure::{CompoundMember, Datatype};
        use proptest::prelude::*;

        /// One stream of a generated export, its route's hops, its id and
        /// the wire type of each column.
        #[derive(Debug, Clone)]
        struct StreamPlan {
            hops: Vec<u8>,
            id: u8,
            types: Vec<DataType>,
        }

        /// One batch of a generated export, its stream, the gap in sample
        /// numbers before it, and a seed per row that its cells' bits come
        /// from.
        #[derive(Debug, Clone)]
        struct BatchPlan {
            stream: usize,
            gap: u32,
            rows: Vec<u64>,
        }

        /// The bits of cell `column` of the row whose seed is `seed`, mixed
        /// as `SplitMix64` mixes, so neighbouring cells share no pattern.
        fn cell(seed: u64, column: usize) -> u64 {
            let mut bits = seed ^ (column as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            bits = (bits ^ (bits >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            bits = (bits ^ (bits >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            bits ^ (bits >> 31)
        }

        /// The cells of each row of `plan`, for a stream of `width` columns.
        fn cells(plan: &BatchPlan, width: usize) -> Vec<Vec<u64>> {
            let row = |seed: &u64| (0..width).map(|column| cell(*seed, column)).collect();
            plan.rows.iter().map(row).collect()
        }

        fn native_type() -> impl Strategy<Value = DataType> {
            prop_oneof![
                3 => Just(DataType::U8),
                3 => Just(DataType::I8),
                1 => Just(DataType::U16),
                1 => Just(DataType::I16),
                1 => Just(DataType::U32),
                1 => Just(DataType::I32),
                1 => Just(DataType::U64),
                1 => Just(DataType::I64),
                1 => Just(DataType::F32),
                1 => Just(DataType::F64),
            ]
        }

        /// The column types of a stream, a few narrow ones three times in
        /// five, or else enough to make a row of 256 bytes or more.
        fn column_types() -> impl Strategy<Value = Vec<DataType>> {
            prop_oneof![
                3 => prop::collection::vec(native_type(), 1..=5),
                2 => prop::collection::vec(native_type(), 33..=36),
            ]
        }

        /// One to three streams, each a distinct route and id.
        fn stream_plans() -> impl Strategy<Value = Vec<StreamPlan>> {
            let key = (prop::collection::vec(0u8..3, 0..=2), 1u8..=2);
            prop::collection::btree_map(key, column_types(), 1..=3).prop_map(|streams| {
                streams
                    .into_iter()
                    .map(|((hops, id), types)| StreamPlan { hops, id, types })
                    .collect()
            })
        }

        /// Streams, then batches of them, with the first sample number and
        /// the rate every stream shares.
        fn export_plan() -> impl Strategy<Value = (Vec<StreamPlan>, Vec<BatchPlan>, u32, u32)> {
            stream_plans().prop_flat_map(|streams| {
                let rows = prop_oneof![
                    3 => prop::collection::vec(any::<u64>(), 0..=24),
                    2 => prop::collection::vec(any::<u64>(), 257..=600),
                ];
                let batch = (0..streams.len(), 0u32..4, rows)
                    .prop_map(|(stream, gap, rows)| BatchPlan { stream, gap, rows });
                (
                    Just(streams),
                    prop::collection::vec(batch, 1..=8),
                    0u32..=u32::MAX - 8 * 1024,
                    1u32..=1000,
                )
            })
        }

        /// Appends one row's cells to `column`, each the low bytes of its
        /// bits in the column's native type.
        fn push_cells(column: &mut ColumnBuilder, cells: impl Iterator<Item = u64>) {
            macro_rules! push {
                ($($variant:ident $native:ty),* $(,)?) => {
                    match column {
                        $(
                            ColumnBuilder::$variant(values) => values.extend(cells.map(|bits| {
                                let bytes = bits.to_le_bytes();
                                <$native>::from_le_bytes(
                                    bytes[..size_of::<$native>()].try_into().unwrap(),
                                )
                            })),
                        )*
                    }
                };
            }
            native_types!(push)
        }

        /// The little endian bytes of row `row` of `values`.
        fn cell_bytes(values: &ColumnArray, row: usize) -> Vec<u8> {
            macro_rules! bytes {
                ($($variant:ident $native:ty),* $(,)?) => {
                    match values {
                        $(ColumnArray::$variant(values) => values[row].to_le_bytes().to_vec(),)*
                    }
                };
            }
            native_types!(bytes)
        }

        /// The HDF5 type of a column of `data_type`, as `hdf5-pure` builds it.
        fn hdf_type(data_type: DataType) -> Datatype {
            match data_type {
                DataType::U8 => hdf5_pure::make_u8_type(),
                DataType::I8 => hdf5_pure::make_i8_type(),
                DataType::U16 => hdf5_pure::make_u16_type(),
                DataType::I16 => hdf5_pure::make_i16_type(),
                DataType::U32 => hdf5_pure::make_u32_type(),
                DataType::I32 => hdf5_pure::make_i32_type(),
                DataType::U64 => hdf5_pure::make_u64_type(),
                DataType::I64 => hdf5_pure::make_i64_type(),
                DataType::F32 => hdf5_pure::make_f32_type(),
                DataType::F64 => hdf5_pure::make_f64_type(),
                other => panic!("no native type {other:?}"),
            }
        }

        /// A batch of the stream `plan` describes, rows numbered from
        /// `first`.
        fn plan_batch(plan: &StreamPlan, rows: &[Vec<u64>], first: u32, rate: u32) -> SampleBatch {
            let id = plan.id;
            let names: Vec<String> = (0..plan.types.len()).map(|i| format!("c{i}")).collect();
            let columns: Vec<wire::Column<'_>> = plan
                .types
                .iter()
                .zip(&names)
                .enumerate()
                .map(|(index, (&data_type, name))| wire::Column {
                    name,
                    ..fixtures::column(id, index as u8, data_type)
                })
                .collect();
            let stream_name = format!("s{id}");
            let metadata = fixtures::metadata(
                DeviceRoute::from_hops(&plan.hops).unwrap(),
                wire::Device {
                    n_streams: 3,
                    ..fixtures::device()
                },
                wire::Stream {
                    name: &stream_name,
                    n_columns: columns.len() as u8,
                    ..fixtures::stream(id)
                },
                wire::Segment {
                    sampling_rate: rate,
                    start_time: first % 1000,
                    ..fixtures::segment(id)
                },
                &columns,
            );
            let mut builder = SampleBatchBuilder::new(
                BatchContext {
                    metadata,
                    boundary: None,
                    generations: Generations {
                        stream: 1,
                        device: 0,
                        global: 0,
                    },
                },
                rows.len(),
            );
            let columns = builder.push_samples(SampleNumber::new(first), rows.len());
            for (index, column) in columns.enumerate() {
                push_cells(column, rows.iter().map(|row| row[index]));
            }
            builder.finish()
        }

        /// The table of stream `id` at `hops`, as the module doc's tree
        /// places it.
        fn table_path(hops: &[u8], id: u8) -> String {
            let groups: Vec<String> = hops.iter().map(u8::to_string).collect();
            let name = format!("s{id}");
            match groups.is_empty() {
                true => name,
                false => format!("{}/{name}", groups.join("/")),
            }
        }

        /// The size and fields of `table`'s compound type.
        fn compound(table: &Dataset) -> (u64, Vec<CompoundMember>) {
            match table.datatype().expect("read the table's type") {
                Datatype::Compound { size, members } => (u64::from(size), members),
                other => panic!("a table is a compound, not {other:?}"),
            }
        }

        /// The bytes of the field named `name` in `row`.
        fn field<'a>(members: &[CompoundMember], row: &'a [u8], name: &str) -> &'a [u8] {
            let member = members
                .iter()
                .find(|member| member.name == name)
                .unwrap_or_else(|| panic!("a field named {name}"));
            let at = member.byte_offset as usize;
            &row[at..at + member.datatype.type_size() as usize]
        }

        /// Checks E2 on the fields of one table, whose columns have `types`.
        fn check_c_layout(
            size: u64,
            members: &[CompoundMember],
            types: &[DataType],
        ) -> std::result::Result<(), TestCaseError> {
            let mut expected = vec![
                ("sample".to_string(), hdf5_pure::make_u32_type()),
                ("time".to_string(), hdf5_pure::make_f64_type()),
            ];
            expected.extend(
                types
                    .iter()
                    .enumerate()
                    .map(|(i, &data_type)| (format!("c{i}"), hdf_type(data_type))),
            );
            let mut placed: Vec<&CompoundMember> = members.iter().collect();
            placed.sort_by_key(|member| member.byte_offset);
            let fields: Vec<(String, Datatype)> = placed
                .iter()
                .map(|member| (member.name.clone(), member.datatype.clone()))
                .collect();
            prop_assert_eq!(fields, expected, "the fields in order, each of its type");
            let mut end = 0;
            for member in placed {
                let width = u64::from(member.datatype.type_size());
                let at = member.byte_offset;
                prop_assert!(
                    at.is_multiple_of(width),
                    "{} sits at a multiple of its size",
                    member.name
                );
                prop_assert!(at >= end, "{} overlaps the field before", member.name);
                prop_assert!(
                    at - end < width,
                    "{} has more padding than C adds",
                    member.name
                );
                end = at + width;
            }
            prop_assert!(
                size >= end && size.is_multiple_of(8) && size - end < 8,
                "row size {size}"
            );
            Ok(())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(48))]

            /// E1, E2 and E3. Every field of every row reads back as written,
            /// at the offsets a C struct of the fields has.
            #[test]
            fn every_field_of_every_row_reads_back_as_written(
                (streams, batches, start, rate) in export_plan(),
                compress in any::<bool>(),
            ) {
                let _guard = hdf_test_lock();
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("export.h5");
                let mut writer = Hdf5Appender::with_options(
                    &path,
                    compress,
                    None,
                    SplitPolicy::Continuous,
                    RunSplitLevel::None,
                )
                .unwrap();
                let mut next: Vec<u32> = vec![start; streams.len()];
                let mut written: Vec<Vec<SampleBatch>> = vec![Vec::new(); streams.len()];
                for plan in &batches {
                    let first = next[plan.stream] + plan.gap;
                    next[plan.stream] = first + plan.rows.len() as u32;
                    let stream = &streams[plan.stream];
                    let rows = cells(plan, stream.types.len());
                    let batch = plan_batch(stream, &rows, first, rate);
                    writer.write_batch(batch.clone()).unwrap();
                    written[plan.stream].push(batch);
                }
                let stats = writer.finish().unwrap();
                let rows: usize = batches.iter().map(|plan| plan.rows.len()).sum();
                prop_assert_eq!(stats.total_samples, rows as u64);

                let file = File::open(&path).unwrap();
                for (plan, batches) in streams.iter().zip(&written) {
                    let path = table_path(&plan.hops, plan.id);
                    let expected: usize = batches.iter().map(SampleBatch::len).sum();
                    let table = match (batches.is_empty(), file.dataset(&path)) {
                        (true, found) => {
                            prop_assert!(found.is_err(), "{} has a table but no batch", path);
                            continue;
                        }
                        (false, Err(_)) if expected == 0 => continue,
                        (false, found) => found.unwrap(),
                    };
                    let (size, members) = compound(&table);
                    check_c_layout(size, &members, &plan.types)?;
                    prop_assert_eq!(table.shape().unwrap(), vec![expected as u64]);
                    let raw = table.read_raw().unwrap();
                    let mut rows = raw.chunks_exact(size as usize);
                    for batch in batches {
                        for i in 0..batch.len() {
                            let row = rows.next().expect("a row per sample");
                            prop_assert_eq!(
                                field(&members, row, "sample"),
                                &batch.sample_numbers()[i].value().to_le_bytes()[..],
                                "{} sample of row {}", path, i
                            );
                            prop_assert_eq!(
                                field(&members, row, "time"),
                                &batch.timestamps()[i].to_le_bytes()[..],
                                "{} time of row {}", path, i
                            );
                            for (column, values) in batch.columns().iter().enumerate() {
                                prop_assert_eq!(
                                    field(&members, row, &format!("c{column}")),
                                    &cell_bytes(values, i)[..],
                                    "{} column {} of row {}", path, column, i
                                );
                            }
                        }
                    }
                }
            }
        }

        /// The layout of a table of five columns and one row, as a C compiler
        /// lays out the struct of its fields. Sample 7 of a 1 Hz segment that
        /// starts at 7 s ends at 15 s.
        ///
        /// ```text
        /// struct row { uint32_t sample; double time; uint8_t c0; int16_t c1;
        ///              uint8_t c2; double c3; int8_t c4; };
        /// ```
        #[test]
        fn a_table_is_laid_out_as_its_c_struct() {
            let _guard = hdf_test_lock();
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("literal.h5");
            let plan = StreamPlan {
                hops: vec![0],
                id: 1,
                types: vec![
                    DataType::U8,
                    DataType::I16,
                    DataType::U8,
                    DataType::F64,
                    DataType::I8,
                ],
            };
            let cells = vec![0xAB, 0xFFFF_FFFF_FFFF_8001, 0xCD, 2.5f64.to_bits(), 0xFE];
            let batch = plan_batch(&plan, &[cells], 7, 1);
            let mut writer = appender(&path);
            writer.write_batch(batch).unwrap();
            writer.finish().unwrap();

            let file = File::open(&path).unwrap();
            let table = file.dataset("0/s1").unwrap();
            let (size, members) = compound(&table);
            let offsets: Vec<(&str, u64)> = members
                .iter()
                .map(|member| (member.name.as_str(), member.byte_offset))
                .collect();
            assert_eq!(
                offsets,
                [
                    ("sample", 0),
                    ("time", 8),
                    ("c0", 16),
                    ("c1", 18),
                    ("c2", 20),
                    ("c3", 24),
                    ("c4", 32),
                ]
            );
            assert_eq!(size, 40);
            #[rustfmt::skip]
            let row = [
                7, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0x2E, 0x40,
                0xAB, 0, 0x01, 0x80, 0xCD, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0x04, 0x40,
                0xFE, 0, 0, 0, 0, 0, 0, 0,
            ];
            assert_eq!(table.read_raw().unwrap(), row);
        }
    }
}

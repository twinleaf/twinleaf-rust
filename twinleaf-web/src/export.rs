//! Export of the hub's raw history to a file the page saves: every raw row
//! of the chosen traces within a time range, as CSV or HDF5. The hub hands
//! over the rows' batches, which share its chunks, and lets go of its lock
//! before a byte is written, so a large export never holds up the plots.
//!
//! CSV is one table, the merged table of
//! [`write_merged`](twinleaf::data::csv::write_merged): a time column, then
//! one column per trace, a row per distinct time. It is written as it is
//! read.
//!
//! HDF5 is the [`Hdf5Appender`](twinleaf::data::export::Hdf5Appender)
//! layout `tio log` writes: a table per stream, every column of it. An HDF5
//! file is written whole before it can be read, so it goes to a temporary
//! file first, which is sent and then removed.

/// Writes `traces`' streams as HDF5 to a new file at `path`, each stream
/// once however many of its columns are traced.
#[cfg(feature = "hdf5")]
pub(crate) fn write_hdf5(
    traces: Vec<twinleaf::data::csv::MergedColumn>,
    path: &std::path::Path,
) -> Result<(), String> {
    use twinleaf::data::export::{Hdf5Appender, RunSplitLevel, SplitPolicy};
    let mut writer = Hdf5Appender::with_options(
        path,
        true,
        None,
        SplitPolicy::default(),
        RunSplitLevel::default(),
    )
    .map_err(|e| e.to_string())?;
    let mut written = std::collections::HashSet::new();
    for trace in traces {
        let Some(first) = trace.batches.first() else {
            continue;
        };
        if !written.insert(first.stream_key()) {
            continue;
        }
        for batch in trace.batches {
            writer.write_batch(batch).map_err(|e| e.to_string())?;
        }
    }
    writer.finish().map_err(|e| e.to_string())?;
    Ok(())
}

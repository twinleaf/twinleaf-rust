//! Export of the hub's raw history to a file the page saves: every raw row
//! of the chosen traces within a time range, as CSV or HDF5. The hub hands
//! over the rows' batches, which share its chunks, and lets go of its lock
//! before a byte is written, so a large export never holds up the plots.
//!
//! CSV is one table: a time column, then one column per trace, a row per
//! distinct time with each trace's value at that time and nothing where the
//! trace has no sample then. It is written as it is read, a chunk at a time.
//!
//! HDF5 is the [`Hdf5Appender`](twinleaf::data::export::Hdf5Appender)
//! layout `tio log` writes: a table per stream, every column of it. An HDF5
//! file is written whole before it can be read, so it goes to a temporary
//! file first, which is sent and then removed.

use std::io::{self, Write};
use twinleaf::data::SampleBatch;

/// One trace to export: its name and units, and its raw rows in time order.
pub(crate) struct ExportTrace {
    /// The trace as the page names it, as `stream.column` or `0/stream.column`.
    pub(crate) name: String,
    pub(crate) units: String,
    /// Whether the trace's time is Unix seconds, as its segment says.
    pub(crate) unix: bool,
    /// The column of the batches the trace reads.
    pub(crate) column: twinleaf::proto::ColumnId,
    pub(crate) batches: Vec<SampleBatch>,
}

/// A value as text: nine significant digits, which give back a 32-bit
/// float exactly, without the digits widening it to 64 bits invents. The
/// page's copy writes values the same way.
fn value(v: f64) -> String {
    match format!("{v:.8e}").parse::<f64>() {
        Ok(rounded) => rounded.to_string(),
        Err(_) => v.to_string(),
    }
}

/// One trace's rows, read a row at a time.
struct Cursor {
    times: Vec<f64>,
    values: Vec<f64>,
    at: usize,
}

impl Cursor {
    fn of(trace: &ExportTrace) -> Self {
        let (mut times, mut values) = (Vec::new(), Vec::new());
        for batch in &trace.batches {
            if let Some(column) = batch.column(trace.column) {
                times.extend_from_slice(batch.timestamps());
                values.extend(column.to_f64().iter());
            }
        }
        Self {
            times,
            values,
            at: 0,
        }
    }

    fn time(&self) -> Option<f64> {
        self.times.get(self.at).copied()
    }
}

/// Writes `traces` as CSV to `out`: a header of the time and each trace
/// with its units, then a row per distinct time, oldest first, merging the
/// traces' rows by time.
pub(crate) fn write_csv(traces: &[ExportTrace], out: &mut impl Write) -> io::Result<()> {
    let time = if traces.iter().all(|t| t.unix) {
        "time (Unix s)"
    } else {
        "time (s)"
    };
    let mut header = vec![time.to_string()];
    header.extend(traces.iter().map(|t| match t.units.as_str() {
        "" => t.name.clone(),
        units => format!("{} ({units})", t.name),
    }));
    writeln!(out, "{}", header.join(","))?;
    let mut cursors: Vec<Cursor> = traces.iter().map(Cursor::of).collect();
    let mut line = String::new();
    while let Some(t) = cursors.iter().filter_map(Cursor::time).reduce(f64::min) {
        line.clear();
        line.push_str(&t.to_string());
        for cursor in &mut cursors {
            line.push(',');
            if cursor.time() == Some(t) {
                line.push_str(&value(cursor.values[cursor.at]));
                cursor.at += 1;
            }
        }
        line.push('\n');
        out.write_all(line.as_bytes())?;
    }
    out.flush()
}

/// Writes `traces`' streams as HDF5 to a new file at `path`, each stream
/// once however many of its columns are traced.
#[cfg(feature = "hdf5")]
pub(crate) fn write_hdf5(traces: Vec<ExportTrace>, path: &std::path::Path) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_keeps_nine_significant_digits() {
        assert_eq!(value(f64::from(0.1f32)), "0.100000001");
        assert_eq!(value(2.0), "2");
        assert_eq!(value(-1234.5678), "-1234.5678");
    }
}

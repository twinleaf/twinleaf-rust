//! Export of the hub's raw history to a file: every raw row of the chosen
//! traces within a time range, as CSV or HDF5. The hub hands over the rows'
//! batches, which share its chunks, and lets go of its lock before a byte
//! is written, so a large export never holds up the plots.
//!
//! CSV is one table, the merged table of
//! [`write_merged`](twinleaf::data::csv::write_merged): a time column, then
//! one column per trace, a row per distinct time. The page's export is
//! written as it is read.
//!
//! HDF5 is the [`Hdf5Appender`](twinleaf::data::export::Hdf5Appender)
//! layout `tio log` writes: a table per stream, every column of it. An HDF5
//! file is written whole before it can be read, so the page's export goes
//! to a temporary file first, which is sent and then removed.
//!
//! An export to a file goes to [`folder`], under a name [`path_of`] checks.
//! [`write_new`] claims the file by creating it, so a file there already is
//! never written over, and removes a file it created and failed to write.

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use twinleaf::data::csv::MergedColumn;

/// The file an export writes.
#[derive(Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Debug)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
pub(crate) enum Format {
    /// One table, a time column and then a column per signal.
    Csv,
    /// A table per stream, every column of it.
    Hdf5,
}

impl Format {
    /// The extension of a file of this format.
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Hdf5 => "h5",
        }
    }
}

/// The folder exports to a file go to, `twinleaf-exports` under the system
/// temp folder.
pub(crate) fn folder() -> PathBuf {
    std::env::temp_dir().join("twinleaf-exports")
}

/// The path in [`folder`] of the file `name` names, with the extension of
/// `format` after it unless it ends with that already. Without a name the
/// file is named by the time now. A name that holds a folder separator, a
/// drive or `..`, or that is empty, is refused with a sentence that says
/// why. The folder is made when it does not exist.
pub(crate) fn path_of(name: Option<&str>, format: Format) -> Result<PathBuf, String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static EXPORTS: AtomicU64 = AtomicU64::new(0);
    let extension = format.extension();
    let file = match name {
        Some(name) => {
            if name.trim().is_empty() {
                return Err("a name of a file holds at least one character".into());
            }
            if name.contains(['/', '\\']) {
                return Err(format!(
                    "{name:?} holds a folder separator, and a name holds no folder"
                ));
            }
            if name.contains(':') {
                return Err(format!(
                    "{name:?} holds a colon, which names a drive, and a name holds none"
                ));
            }
            if name.contains("..") {
                return Err(format!(
                    "{name:?} holds .., and a name holds no folder above"
                ));
            }
            match name.ends_with(&format!(".{extension}")) {
                true => name.to_string(),
                false => format!("{name}.{extension}"),
            }
        }
        None => {
            let now = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH))
                .map_or(0, |since| since.as_millis());
            let n = EXPORTS.fetch_add(1, Ordering::Relaxed);
            format!("export-{now}-{n}.{extension}")
        }
    };
    let folder = folder();
    std::fs::create_dir_all(&folder)
        .map_err(|error| format!("{} could not be made, as {error}", folder.display()))?;
    Ok(folder.join(file))
}

/// Writes `columns` as `format` to `path`, a file this call creates, and
/// returns its bytes. A file at `path` already is refused and left as it
/// is. A file this call created and then failed to write is removed.
pub(crate) fn write_new(
    columns: Vec<MergedColumn>,
    format: Format,
    path: &Path,
) -> Result<u64, String> {
    let mut created = false;
    let written = match format {
        Format::Csv => write_csv(columns, path, &mut created),
        #[cfg(feature = "hdf5")]
        Format::Hdf5 => write_hdf5(columns, path, &mut created),
        #[cfg(not(feature = "hdf5"))]
        Format::Hdf5 => Err("this hub was built without HDF5, so it writes csv only".into()),
    };
    match written {
        Ok(()) => Ok(std::fs::metadata(path).map_or(0, |m| m.len())),
        Err(error) => {
            if created {
                let _ = std::fs::remove_file(path);
            }
            Err(error)
        }
    }
}

/// What an export says when it cannot create `path`, as `error` says.
fn unmade(path: &Path, error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::AlreadyExists => {
            format!(
                "{} exists, and an export writes only a new file",
                path.display()
            )
        }
        _ => format!("{} could not be made, as {error}", path.display()),
    }
}

/// Writes `columns` as the merged CSV table to the new file `path`, and
/// says in `created` whether it made the file.
fn write_csv(columns: Vec<MergedColumn>, path: &Path, created: &mut bool) -> Result<(), String> {
    let file = std::fs::File::create_new(path).map_err(|error| unmade(path, &error))?;
    *created = true;
    let mut out = std::io::BufWriter::new(file);
    twinleaf::data::csv::write_merged(&columns, &mut out).map_err(|e| e.to_string())?;
    std::io::Write::flush(&mut out).map_err(|e| e.to_string())
}

/// Writes `traces`' streams as HDF5 to the new file `path`, each stream
/// once however many of its columns are traced, and says in `created`
/// whether it made the file.
#[cfg(feature = "hdf5")]
fn write_hdf5(traces: Vec<MergedColumn>, path: &Path, created: &mut bool) -> Result<(), String> {
    use twinleaf::data::export::{ExportError, Hdf5Appender, RunSplitLevel, SplitPolicy};
    let options = (SplitPolicy::default(), RunSplitLevel::default());
    let mut writer = match Hdf5Appender::with_options(path, true, None, options.0, options.1) {
        Ok(writer) => writer,
        Err(ExportError::Create { source, .. }) => return Err(unmade(path, &source)),
        Err(error) => return Err(error.to_string()),
    };
    *created = true;
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
    fn a_name_is_a_file_in_the_folder_and_nothing_else() {
        let path = path_of(Some("run 1"), Format::Csv).unwrap();
        assert_eq!(path, folder().join("run 1.csv"));
        let path = path_of(Some("run.h5"), Format::Hdf5).unwrap();
        assert_eq!(path, folder().join("run.h5"));
        let timed = path_of(None, Format::Csv).unwrap();
        assert_eq!(timed.parent(), Some(folder().as_path()));
        for name in ["a/b", "a\\b", "C:x", "..", "a..b", "", " "] {
            let refused = path_of(Some(name), Format::Csv);
            assert!(refused.is_err(), "{name:?} gave {refused:?}");
        }
    }

    #[test]
    fn a_file_there_already_is_refused_and_kept() {
        let path = folder().join(format!("twinleaf-export-test-{}.csv", std::process::id()));
        std::fs::create_dir_all(folder()).unwrap();
        std::fs::write(&path, b"mine").unwrap();
        let refused = write_new(Vec::new(), Format::Csv, &path).unwrap_err();
        assert!(refused.contains("exists"), "{refused}");
        assert_eq!(std::fs::read(&path).unwrap(), b"mine");
        std::fs::remove_file(&path).unwrap();
    }
}

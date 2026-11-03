//! The on-disk cache of RPC registries, the RPC layer's only file access.
//!
//! The crate's own tests have no cache directory, so none of them reads or
//! writes the user's. A test of the cache holds a `Scratch` directory.

use crc::{Crc, CRC_32_ISO_HDLC};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
pub(crate) use scratch::Scratch;

pub(super) type Entries = Vec<(String, u16)>;

const CACHE_CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

/// Returns the cache file of a device's registry, or `None` when no usable
/// cache directory exists.
// TODO: evict stale cache files from old firmware versions (<dev_name>.*.rpcs)
pub(crate) fn path(dev_name: &str, hash: u32) -> Option<PathBuf> {
    let dir = dir()?;
    if let Err(error) = fs::create_dir_all(&dir) {
        log::warn!(
            "could not create RPC cache directory {}: {error}",
            dir.display()
        );
        return None;
    }
    Some(dir.join(format!("{}.{hash:x}.rpcs", stem(dev_name))))
}

/// Returns the `twinleaf` directory in the user's cache directory.
#[cfg(not(test))]
fn dir() -> Option<PathBuf> {
    let Some(dirs) = directories::BaseDirs::new() else {
        log::debug!("no cache directory, so RPC tables are not cached");
        return None;
    };
    Some(dirs.cache_dir().join("twinleaf"))
}

/// Returns the [`Scratch`] directory this thread holds, or `None`, which
/// leaves the test without a cache.
#[cfg(test)]
fn dir() -> Option<PathBuf> {
    scratch::current()
}

#[cfg(test)]
mod scratch {
    use std::cell::RefCell;
    use std::marker::PhantomData;
    use std::path::{Path, PathBuf};

    thread_local! {
        static CURRENT: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    pub(super) fn current() -> Option<PathBuf> {
        CURRENT.with(|current| current.borrow().clone())
    }

    /// A fresh cache directory, in force on the thread that made it until it
    /// drops. Dropping it removes the directory and puts back the one the
    /// thread held before, which outside another `Scratch` is none.
    pub(crate) struct Scratch {
        dir: tempfile::TempDir,
        previous: Option<PathBuf>,
        /// The directory is in force on one thread, so the guard stays there.
        _thread: PhantomData<*const ()>,
    }

    impl Scratch {
        pub(crate) fn new() -> Scratch {
            let dir = tempfile::tempdir().expect("a temporary directory");
            let held = Some(dir.path().to_path_buf());
            let previous = CURRENT.with(|current| current.replace(held));
            Scratch {
                dir,
                previous,
                _thread: PhantomData,
            }
        }

        /// Returns the directory.
        pub(crate) fn path(&self) -> &Path {
            self.dir.path()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let previous = self.previous.take();
            CURRENT.with(|current| current.replace(previous));
        }
    }
}

/// Returns the cache file stem for a device's name, restricted to characters
/// that cannot leave the cache directory.
fn stem(dev_name: &str) -> String {
    dev_name
        .chars()
        .take(64)
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

/// Loads the cached entries at `path`, or `None` when nothing usable is
/// there. Deletes a file that does not parse, and logs any failure to read.
pub(super) fn load(path: &Path) -> Option<Entries> {
    let read = match File::open(path) {
        Ok(file) => read(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => Err(error),
    };
    match read {
        Ok(Some(entries)) => Some(entries),
        Ok(None) => {
            warn(fs::remove_file(path), "discard stale", path);
            None
        }
        Err(error) => {
            warn(Err(error), "read", path);
            None
        }
    }
}

/// Writes the cache, logging any failure without returning it.
pub(super) fn store(path: &Path, entries: &Entries) {
    warn(write(path, entries), "write", path);
}

fn warn(result: io::Result<()>, action: &str, path: &Path) {
    if let Err(error) = result {
        log::warn!("could not {action} RPC cache {}: {error}", path.display());
    }
}

/// Reads the cache format. Returns `None` for contents that do not parse or
/// fail their checksum.
fn read(file: File) -> io::Result<Option<Entries>> {
    let reader = io::BufReader::new(file);
    let mut lines = reader.lines();
    let mut entries = Vec::new();
    let mut digest = CACHE_CRC.digest();
    let mut stored_crc = None;

    while let Some(line) = lines.next() {
        let line = line?;
        let Some((meta, name)) = line.split_once(' ') else {
            stored_crc = u32::from_str_radix(&line, 16).ok();
            if lines.next().transpose()?.is_some() {
                return Ok(None);
            }
            break;
        };
        let Some(meta) = u16::from_str_radix(meta, 16).ok() else {
            return Ok(None);
        };
        digest.update(line.as_bytes());
        digest.update(b"\n");
        entries.push((name.trim().to_string(), meta));
    }

    Ok((stored_crc == Some(digest.finalize())).then_some(entries))
}

fn temporary_path(path: &Path, id: u64) -> PathBuf {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), id))
}

fn create_temporary(path: &Path) -> io::Result<(PathBuf, File)> {
    loop {
        let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = temporary_path(path, id);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

/// Writes a complete cache file and renames it into place at `path`.
fn write(path: &Path, entries: &[(String, u16)]) -> io::Result<()> {
    let (temporary, file) = create_temporary(path)?;
    let result = (|| {
        let mut writer = io::BufWriter::new(file);
        let mut digest = CACHE_CRC.digest();

        for (name, meta) in entries {
            let line = format!("{meta:04x} {name}\n");
            writer.write_all(line.as_bytes())?;
            digest.update(line.as_bytes());
        }
        writeln!(writer, "{:08x}", digest.finalize())?;
        writer.flush()?;
        drop(writer.into_inner().map_err(|error| error.into_error())?);

        fs::rename(&temporary, path)
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{load, path, stem, store, Scratch};

    /// A test has no cache until it holds a scratch directory, which holds
    /// the cache until it drops and is then removed.
    #[test]
    fn a_test_has_no_cache_outside_a_scratch_directory() {
        assert_eq!(path("test-device", 0x1234), None);

        let outer = Scratch::new();
        let in_outer = path("test-device", 0x1234).expect("a cache in the scratch directory");
        assert_eq!(in_outer, outer.path().join("test-device.1234.rpcs"));
        let inner = Scratch::new();
        let in_inner = path("test-device", 0x1234).expect("a cache in the scratch directory");
        assert_eq!(in_inner, inner.path().join("test-device.1234.rpcs"));
        let inner_dir = inner.path().to_path_buf();
        drop(inner);
        assert!(!inner_dir.exists());
        assert_eq!(path("test-device", 0x1234), Some(in_outer));

        drop(outer);
        assert_eq!(path("test-device", 0x1234), None);
    }

    /// A cache that cannot be read is a miss, so the table is walked again.
    #[test]
    fn an_unreadable_cache_is_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).is_none());
    }

    /// A stored table loads back, and a file that does not parse is a miss
    /// that is deleted.
    #[test]
    fn a_stored_table_loads_back_and_garbage_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("device.1234.rpcs");
        let entries = vec![("dev.name".to_string(), 0x0123)];
        store(&path, &entries);
        assert_eq!(load(&path), Some(entries));

        std::fs::write(&path, b"not a cache").unwrap();
        assert!(load(&path).is_none());
        assert!(!path.exists());
    }

    #[test]
    fn stem_cannot_escape_the_cache_directory() {
        assert_eq!(stem("../../etc/passwd"), "______etc_passwd");
        assert_eq!(stem("sync-v2"), "sync-v2");
    }
}

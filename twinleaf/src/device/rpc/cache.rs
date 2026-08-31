//! The on-disk RPC registry cache: the only part of the RPC layer that touches
//! a filesystem.

use crc::{Crc, CRC_32_ISO_HDLC};
use directories::BaseDirs;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) type Entries = Vec<(String, u16)>;

const CACHE_CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

/// Where a device's registry is cached, `None` when no usable cache directory
/// exists.
// TODO: evict stale cache files from old firmware versions (<dev_name>.*.rpcs)
pub(super) fn path(dev_name: &str, hash: u32) -> Option<PathBuf> {
    let dir = BaseDirs::new()?.cache_dir().join("twinleaf");
    fs::create_dir_all(&dir).ok()?;
    Some(dir.join(format!("{}.{hash:x}.rpcs", stem(dev_name))))
}

/// Cache filename stem for a device-supplied name. The name is untrusted — a
/// network device could report `../..` — so keep it to a charset that cannot
/// escape the cache directory; the hash still makes the filename unique.
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

/// The cached entries for `path`, or `None` when there is nothing usable there.
pub(super) fn load(path: &Path) -> io::Result<Option<Entries>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match read(file)? {
        Some(entries) => Ok(Some(entries)),
        None => {
            warn(fs::remove_file(path), "discard stale", path);
            Ok(None)
        }
    }
}

/// Keeping the cache tidy is an optimization, never a reason to fail a registry
/// the device already answered for — an unwritable cache directory only costs a
/// round-trip next time.
pub(super) fn store(path: &Path, entries: &Entries) {
    warn(write(path, entries), "write", path);
}

fn warn(result: io::Result<()>, action: &str, path: &Path) {
    if let Err(error) = result {
        log::warn!("could not {action} RPC cache {}: {error}", path.display());
    }
}

/// Read the private on-disk RPC cache format.
///
/// Invalid contents are a cache miss rather than a user-facing error: the
/// caller can discard the file and fetch a fresh registry from the device.
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

/// Write a complete cache file and atomically publish it at `path`.
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
    use super::stem;

    #[test]
    fn stem_cannot_escape_the_cache_directory() {
        assert_eq!(stem("../../etc/passwd"), "______etc_passwd");
        assert_eq!(stem("sync-v2"), "sync-v2");
    }
}

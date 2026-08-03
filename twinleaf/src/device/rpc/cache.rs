use crc::{Crc, CRC_32_ISO_HDLC};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) type Entries = Vec<(String, u16)>;

const CACHE_CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

/// Read the private on-disk RPC cache format.
///
/// Invalid contents are a cache miss rather than a user-facing error: the
/// caller can discard the file and fetch a fresh registry from the device.
pub(super) fn read(file: File) -> io::Result<Option<Entries>> {
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
pub(super) fn write(path: &Path, entries: &[(String, u16)]) -> io::Result<()> {
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

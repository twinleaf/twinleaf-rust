//! A catalog in a folder, and the choice by [`DIR_VARIABLE`] between it and
//! the published repository.

use super::github::GithubCatalog;
use super::{default_cache_dir, FirmwareCatalog, FirmwareError, FirmwareRelease};
use std::path::PathBuf;
use std::sync::Arc;

/// The environment variable that names a folder to read releases from in
/// place of the published repository.
pub const DIR_VARIABLE: &str = "TWINLEAF_FIRMWARE_DIR";

/// Returns the catalog [`DIR_VARIABLE`] chooses and the folder that caches
/// its downloads. A folder the variable names that lists is the catalog, with
/// no cache. Otherwise it is the published repository, cached in [`default_cache_dir`].
pub fn catalog_from_env() -> (Arc<dyn FirmwareCatalog + Send + Sync>, Option<PathBuf>) {
    match listable(std::env::var_os(DIR_VARIABLE)) {
        Ok(Some(dir)) => return (Arc::new(DirectoryCatalog(dir)), None),
        Ok(None) => {}
        Err(warning) => log::warn!("{warning}"),
    }
    (Arc::new(GithubCatalog::twinleaf()), default_cache_dir())
}

/// Returns the folder `dir` names, `None` for no name, or why it cannot be
/// listed.
fn listable(dir: Option<std::ffi::OsString>) -> Result<Option<PathBuf>, String> {
    let Some(dir) = dir.filter(|dir| !dir.is_empty()).map(PathBuf::from) else {
        return Ok(None);
    };
    match std::fs::read_dir(&dir) {
        Ok(_) => Ok(Some(dir)),
        Err(error) => Err(format!(
            "{DIR_VARIABLE} names {}, which cannot be listed ({error}), so releases come from the published repository",
            dir.display()
        )),
    }
}

/// A catalog in a folder laid out as the published repository is,
/// `{name}/{revision}/{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`.
/// A name and revision with no folder have no release.
#[derive(Debug)]
pub struct DirectoryCatalog(pub PathBuf);

impl FirmwareCatalog for DirectoryCatalog {
    fn list_releases(
        &self,
        name: &str,
        revision: &str,
    ) -> Result<Vec<FirmwareRelease>, FirmwareError> {
        if !self.0.is_dir() {
            return Err(FirmwareError::Catalog(format!(
                "{} is no directory",
                self.0.display()
            )));
        }
        let folder = self.0.join(name).join(revision);
        let unlisted = |error: std::io::Error| {
            FirmwareError::Catalog(format!("cannot list {}: {error}", folder.display()))
        };
        let entries = match std::fs::read_dir(&folder) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(unlisted(error)),
        };
        let mut releases = Vec::new();
        for entry in entries {
            let entry = entry.map_err(unlisted)?;
            if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
                continue;
            }
            let Some(file) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let path = entry.path().to_string_lossy().into_owned();
            releases.extend(FirmwareRelease::parse_filename(
                name, revision, &file, &path,
            ));
        }
        Ok(releases)
    }

    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        std::fs::read(&release.url).map_err(|error| {
            FirmwareError::Catalog(format!("cannot read {}: {error}", release.url))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A folder of the test's own, empty, under the system's temporary folder.
    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    /// A directory catalog lists the releases in the folder of a name and
    /// revision, and none for a name and revision with no folder. A root
    /// that is no directory cannot be listed.
    #[test]
    fn a_directory_is_a_catalog_laid_out_as_the_repository() {
        let root = scratch("twinleaf-firmware-dir");
        let folder = root.join("tio-test").join("R1");
        std::fs::create_dir_all(folder.join("nested")).unwrap();
        let name = "tio-test-R1-firmware-2026-06-08-000002.bin";
        std::fs::write(folder.join(name), b"image").unwrap();
        std::fs::write(folder.join("README.md"), b"not firmware").unwrap();
        let catalog = DirectoryCatalog(root.clone());

        let listed = catalog.list_releases("tio-test", "R1").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (listed[0].filename.as_str(), listed[0].short_hash.as_str()),
            (name, "000002")
        );
        assert_eq!(catalog.download(&listed[0]).unwrap(), b"image");
        assert!(catalog.list_releases("tio-test", "R2").unwrap().is_empty());

        let missing = DirectoryCatalog(root.join("missing"));
        assert!(matches!(
            missing.list_releases("tio-test", "R1"),
            Err(FirmwareError::Catalog(_))
        ));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A folder the variable names that lists is the catalog.
    #[test]
    fn a_listable_folder_is_chosen() {
        let root = scratch("twinleaf-firmware-listable");
        assert_eq!(listable(Some(root.clone().into())), Ok(Some(root.clone())));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A name that cannot be listed, missing or no folder, falls back to the
    /// published repository with a warning that names it.
    #[test]
    fn an_unlistable_folder_falls_back_with_a_warning() {
        let root = scratch("twinleaf-firmware-unlistable");
        let file = root.join("file");
        std::fs::write(&file, b"").unwrap();
        for dir in [root.join("missing"), file] {
            let warning = listable(Some(dir.clone().into())).unwrap_err();
            assert!(warning.contains(&dir.display().to_string()), "{warning}");
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// The variable unset or empty names no folder.
    #[test]
    fn no_name_is_no_folder() {
        assert_eq!(listable(None), Ok(None));
        assert_eq!(listable(Some("".into())), Ok(None));
    }
}

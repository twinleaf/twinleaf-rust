//! GitHub-backed [`FirmwareCatalog`] (enabled by the `firmware-update` feature).

use super::{FirmwareCatalog, FirmwareDate, FirmwareError, FirmwareRelease};
use std::io::Read;

const HTTP_USER_AGENT: &str = concat!("twinleaf/", env!("CARGO_PKG_VERSION"));

/// A [`FirmwareCatalog`] backed by a public GitHub repository laid out as
/// `{name}/{revision}/{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`.
pub struct GithubCatalog {
    pub owner: String,
    pub repo: String,
}

impl GithubCatalog {
    /// The official Twinleaf published-firmware repository.
    pub fn twinleaf() -> Self {
        Self {
            owner: "twinleaf".into(),
            repo: "twinleaf-published-firmware".into(),
        }
    }
}

/// Parse `{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin` into a release.
fn parse_release(name: &str, revision: &str, filename: &str, url: &str) -> Option<FirmwareRelease> {
    let stem = filename.strip_suffix(".bin")?;
    let after = stem.split_once("firmware-")?.1; // "2026-03-17-5d1494"
    let mut parts = after.splitn(4, '-');
    let year = parts.next()?;
    let month = parts.next()?;
    let day = parts.next()?;
    let hash = parts.next()?;
    if hash.is_empty() {
        return None;
    }
    let date = FirmwareDate::parse(&format!("{year}-{month}-{day}"))?;
    Some(FirmwareRelease {
        name: name.to_string(),
        revision: revision.to_string(),
        date,
        short_hash: hash.to_string(),
        filename: filename.to_string(),
        url: url.to_string(),
    })
}

impl FirmwareCatalog for GithubCatalog {
    fn list_releases(
        &self,
        name: &str,
        revision: &str,
    ) -> Result<Vec<FirmwareRelease>, FirmwareError> {
        let api_url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}/{}",
            self.owner, self.repo, name, revision
        );

        let response = match ureq::get(&api_url)
            .set("User-Agent", HTTP_USER_AGENT)
            .set("Accept", "application/vnd.github+json")
            .call()
        {
            Ok(r) => r,
            // A missing folder simply means nothing is published yet.
            Err(ureq::Error::Status(404, _)) => return Ok(Vec::new()),
            Err(e) => return Err(FirmwareError::Catalog(format!("listing request failed: {e}"))),
        };

        let listing: serde_json::Value = response
            .into_json()
            .map_err(|e| FirmwareError::Catalog(format!("invalid listing response: {e}")))?;
        let entries = listing
            .as_array()
            .ok_or_else(|| FirmwareError::Catalog("unexpected listing format".into()))?;

        let mut releases = Vec::new();
        for entry in entries {
            if entry.get("type").and_then(|v| v.as_str()) != Some("file") {
                continue;
            }
            let Some(filename) = entry.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(url) = entry.get("download_url").and_then(|v| v.as_str()) else {
                continue;
            };
            if let Some(release) = parse_release(name, revision, filename, url) {
                releases.push(release);
            }
        }
        Ok(releases)
    }

    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        let response = ureq::get(&release.url)
            .set("User-Agent", HTTP_USER_AGENT)
            .call()
            .map_err(|e| FirmwareError::Catalog(format!("download request failed: {e}")))?;
        let mut data = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut data)
            .map_err(|e| FirmwareError::Catalog(format!("download read failed: {e}")))?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_release_filename() {
        let r = parse_release(
            "ASM",
            "R6",
            "ASM-R6-firmware-2026-03-17-5d1494.bin",
            "https://x/f.bin",
        )
        .unwrap();
        assert_eq!(r.date, FirmwareDate::parse("2026-03-17").unwrap());
        assert_eq!(r.short_hash, "5d1494");
        assert_eq!(r.filename, "ASM-R6-firmware-2026-03-17-5d1494.bin");
        assert_eq!(r.url, "https://x/f.bin");
    }

    #[test]
    fn rejects_non_firmware_names() {
        assert!(parse_release("ASM", "R6", "README.md", "u").is_none());
        // Missing hash component.
        assert!(parse_release("ASM", "R6", "ASM-R6-firmware-2026-03-17.bin", "u").is_none());
    }

    /// Live end-to-end check against the public firmware repo. Network-gated,
    /// so it is ignored by default; run with `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn live_list_and_download_asm_r6() {
        let catalog = GithubCatalog::twinleaf();
        let releases = catalog.list_releases("ASM", "R6").expect("listing should succeed");
        assert!(!releases.is_empty(), "expected at least one ASM/R6 release");
        let latest = super::super::latest_release(releases).unwrap();
        let data = catalog.download(&latest).expect("download should succeed");
        assert!(!data.is_empty());

        // A name/revision that does not exist yields an empty list, not an error.
        let none = catalog
            .list_releases("NOPE", "R0")
            .expect("missing folder should be Ok(empty)");
        assert!(none.is_empty());
    }
}

//! A [`FirmwareCatalog`] backed by GitHub, enabled by the `firmware-update`
//! feature.

use super::{FirmwareCatalog, FirmwareError, FirmwareRelease};

/// The user agent of every request.
const HTTP_USER_AGENT: &str = concat!("twinleaf/", env!("CARGO_PKG_VERSION"));

/// A [`FirmwareCatalog`] backed by a public GitHub repository laid out as
/// `{name}/{revision}/{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`.
#[derive(Debug)]
pub struct GithubCatalog {
    /// The repository's owner.
    pub owner: String,
    /// The repository's name.
    pub repo: String,
}

impl GithubCatalog {
    /// Returns the catalog of the official Twinleaf published firmware
    /// repository.
    pub fn twinleaf() -> Self {
        Self {
            owner: "twinleaf".into(),
            repo: "twinleaf-firmware-updates".into(),
        }
    }
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

        let mut response = match ureq::get(&api_url)
            .header("User-Agent", HTTP_USER_AGENT)
            .header("Accept", "application/vnd.github+json")
            .call()
        {
            Ok(r) => r,
            // A missing folder simply means nothing is published yet.
            Err(ureq::Error::StatusCode(404)) => return Ok(Vec::new()),
            Err(e) => {
                return Err(FirmwareError::Catalog(format!(
                    "listing request failed: {e}"
                )))
            }
        };

        let listing: serde_json::Value = response
            .body_mut()
            .read_json()
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
            if let Some(release) = FirmwareRelease::parse_filename(name, revision, filename, url) {
                releases.push(release);
            }
        }
        Ok(releases)
    }

    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        let data = ureq::get(&release.url)
            .header("User-Agent", HTTP_USER_AGENT)
            .call()
            .map_err(|e| FirmwareError::Catalog(format!("download request failed: {e}")))?
            .into_body()
            .into_with_config()
            .limit(64 * 1024 * 1024)
            .read_to_vec()
            .map_err(|e| FirmwareError::Catalog(format!("download read failed: {e}")))?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Live end-to-end check against the public firmware repo. Network-gated,
    /// so it is ignored by default. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn live_list_and_download_asm_r6() {
        let catalog = GithubCatalog::twinleaf();
        let installed =
            super::super::parse_installed("Twinleaf ASM R6 (000000) [2000-01-01/000000]");
        let report =
            super::super::check_for_update(installed, &catalog).expect("listing should succeed");
        let latest = report.latest.expect("expected at least one ASM/R6 release");
        let data = catalog.download(&latest).expect("download should succeed");
        assert!(!data.is_empty());

        // A name/revision that does not exist yields an empty list, not an error.
        let none = catalog
            .list_releases("NOPE", "R0")
            .expect("missing folder should be Ok(empty)");
        assert!(none.is_empty());
    }
}

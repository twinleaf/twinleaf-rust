//! `tio upgrade` porcelain.
//!
//! All firmware logic (querying the device, checking the catalog, downloading,
//! and flashing) lives in `twinleaf::firmware`. This module only handles user
//! interaction: presenting the installed-vs-latest comparison, prompting, and
//! rendering progress.

use crate::{ProxyHelp, TioOpts, UpgradeCli};
use color_eyre::Help;
use console::style;
use eyre::WrapErr;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Duration;
use twinleaf::firmware::{
    self, github::GithubCatalog, FlashEvent, StopOutcome, UpdateReport, UpdateStatus,
};
use twinleaf::tio::proxy;

pub fn run_upgrade(upgrade_cli: UpgradeCli) -> eyre::Result<()> {
    match (upgrade_cli.firmware_path, upgrade_cli.downgrade) {
        (Some(path), _) => firmware_upgrade(&upgrade_cli.tio, path, upgrade_cli.yes),
        (None, true) => firmware_select(&upgrade_cli.tio),
        (None, false) => firmware_upgrade_latest(&upgrade_cli.tio, upgrade_cli.yes),
    }
}

/// Flash a firmware image from a local file.
pub fn firmware_upgrade(
    tio: &TioOpts,
    firmware_path: PathBuf,
    skip_confirm: bool,
) -> eyre::Result<()> {
    let firmware_data = std::fs::read(&firmware_path)
        .wrap_err_with(|| format!("could not read firmware file {:?}", firmware_path))?;
    log::info!("loaded {} bytes firmware", firmware_data.len());

    let label = firmware_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| firmware_path.display().to_string());

    let (_proxy, device) = open_device(tio)?;

    if !skip_confirm {
        // Best-effort device label from dev.desc; flashing an explicit file
        // should still work even if the description can't be read.
        let target = firmware::query_installed(&device)
            .ok()
            .map(|fw| format!("{} {}", fw.name, fw.revision))
            .unwrap_or_else(|| "the connected device".to_string());

        if !confirm(
            &format!("Upgrade firmware on {} with '{}'?", target, label),
            false,
        )? {
            log::info!("aborted");
            return Ok(());
        }
    }

    flash_with_progress(&device, &firmware_data)
}

/// Detect the sensor, compare against the latest published firmware, and (if
/// newer) download and flash it.
fn firmware_upgrade_latest(tio: &TioOpts, skip_confirm: bool) -> eyre::Result<()> {
    let (_proxy, device) = open_device(tio)?;

    let installed =
        firmware::query_installed(&device).wrap_err("could not read installed firmware info")?;
    let catalog = GithubCatalog::twinleaf();
    let report = firmware::check_for_update(installed, &catalog)
        .wrap_err("firmware update check failed")?;

    let UpdateReport {
        installed,
        latest,
        status,
        ..
    } = report;

    // Present the comparison.
    present_installed(&installed);

    // A development build cannot be replaced by a published release.
    if status == UpdateStatus::DevelopmentBuild {
        print_dev_build_notice();
        return Ok(());
    }

    let label = |s: &str| style(format!("{:11}", s)).bold().cyan();
    match &latest {
        Some(rel) => println!(" {} {}  {}", label("Available:"), rel.date, rel.short_hash),
        None => println!(" {} {}", label("Available:"), style("none published").yellow()),
    }
    println!();

    let release = match status {
        UpdateStatus::DevelopmentBuild => unreachable!("handled above"),
        UpdateStatus::NoPublishedFirmware => {
            println!(
                "{}",
                style(format!(
                    "No published firmware found for {} {}.",
                    installed.name, installed.revision
                ))
                .yellow()
            );
            return Ok(());
        }
        UpdateStatus::UpToDate => {
            println!(
                "{}",
                style("Firmware is up to date — no new firmware available.").green()
            );
            return Ok(());
        }
        UpdateStatus::Unknown => {
            println!(
                "{}",
                style("Could not determine the installed firmware date; comparing is not possible.")
                    .yellow()
            );
            // `latest` is present whenever the status is not NoPublishedFirmware.
            latest.expect("a latest release exists when status is Unknown")
        }
        UpdateStatus::UpdateAvailable => {
            latest.expect("a latest release exists when an update is available")
        }
    };

    if !confirm(
        &format!(
            "Download and upgrade to {} ({})?",
            release.date, release.short_hash
        ),
        skip_confirm,
    )? {
        log::info!("aborted");
        return Ok(());
    }

    download_and_flash(&device, &catalog, &release)
}

/// Detect the sensor, list every published firmware, and let the user pick one
/// to install (including older releases than what is running).
fn firmware_select(tio: &TioOpts) -> eyre::Result<()> {
    let (_proxy, device) = open_device(tio)?;

    let installed =
        firmware::query_installed(&device).wrap_err("could not read installed firmware info")?;
    let catalog = GithubCatalog::twinleaf();
    let report =
        firmware::check_for_update(installed, &catalog).wrap_err("firmware lookup failed")?;

    let UpdateReport {
        installed,
        releases,
        status,
        ..
    } = report;

    present_installed(&installed);

    if status == UpdateStatus::DevelopmentBuild {
        print_dev_build_notice();
        return Ok(());
    }
    if releases.is_empty() {
        println!();
        println!(
            "{}",
            style(format!(
                "No published firmware found for {} {}.",
                installed.name, installed.revision
            ))
            .yellow()
        );
        return Ok(());
    }
    println!();

    // Build picker labels, marking the currently-installed build.
    let items: Vec<String> = releases
        .iter()
        .map(|rel| {
            let installed_here = installed
                .hash
                .as_deref()
                .is_some_and(|h| h.eq_ignore_ascii_case(&rel.short_hash));
            format!(
                "{}  {}{}",
                rel.date,
                rel.short_hash,
                if installed_here { "  (installed)" } else { "" }
            )
        })
        .collect();

    let selection = dialoguer::Select::new()
        .with_prompt("Select firmware to install")
        .items(&items)
        .default(0)
        .interact()
        .wrap_err("failed to read selection")?;
    let release = &releases[selection];

    download_and_flash(&device, &catalog, release)
}

/// Print the `Sensor`/`Device` and `Installed` header lines for a device.
fn present_installed(installed: &firmware::InstalledFirmware) {
    let label = |s: &str| style(format!("{:11}", s)).bold().cyan();
    println!();
    if installed.name.is_empty() {
        println!(" {} {}", label("Device:"), installed.description);
    } else {
        println!(" {} {} {}", label("Sensor:"), installed.name, installed.revision);
    }
    println!(
        " {} {}  {}",
        label("Installed:"),
        installed
            .build_date
            .map(|d| d.to_string())
            .unwrap_or_else(|| "unknown".into()),
        installed.hash.as_deref().unwrap_or("-"),
    );
}

fn print_dev_build_notice() {
    println!();
    println!(
        "{}",
        style(
            "This device is running a development firmware build; \
             it cannot be upgraded to a published release."
        )
        .yellow()
    );
}

/// Download a release (using the cache) and flash it, with progress.
fn download_and_flash(
    device: &proxy::Port,
    catalog: &GithubCatalog,
    release: &firmware::FirmwareRelease,
) -> eyre::Result<()> {
    let cache_root = firmware::default_cache_dir()
        .ok_or_else(|| eyre::eyre!("could not determine a cache directory"))?;
    let firmware_data = download_with_progress(catalog, release, &cache_root)?;
    log::info!("loaded {} bytes firmware", firmware_data.len());

    flash_with_progress(device, &firmware_data)
}

/// Open a device RPC port. The returned [`proxy::Interface`] owns the
/// connection and must be kept alive for as long as the [`proxy::Port`] is
/// used, otherwise the proxy is torn down and the port disconnects.
fn open_device(tio: &TioOpts) -> eyre::Result<(proxy::Interface, proxy::Port)> {
    let proxy = proxy::Interface::new(&tio.root);
    let device = proxy
        .device_rpc(tio.route.clone())
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;
    Ok((proxy, device))
}

fn confirm(prompt: &str, skip_confirm: bool) -> eyre::Result<bool> {
    if skip_confirm {
        return Ok(true);
    }
    dialoguer::Confirm::new()
        .with_prompt(prompt)
        .default(false)
        .interact()
        .wrap_err("failed to read confirmation")
}

fn download_with_progress(
    catalog: &GithubCatalog,
    release: &firmware::FirmwareRelease,
    cache_root: &std::path::Path,
) -> eyre::Result<Vec<u8>> {
    let spinner = crate::multi_progress().add(ProgressBar::new_spinner());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message(format!("Downloading firmware {}...", release.filename));
    spinner.enable_steady_tick(Duration::from_millis(100));

    let result = firmware::download_cached(catalog, release, cache_root);
    spinner.finish_and_clear();
    let data = result.wrap_err("failed to download firmware")?;

    // Report what was fetched and where it lives.
    let path = firmware::cache_path(cache_root, release);
    let label = |s: &str| style(format!("{:11}", s)).bold().cyan();
    println!(" {} {}", label("Downloaded:"), release.filename);
    println!(" {} {}", label("Location:"), path.display());

    Ok(data)
}

/// Drive [`firmware::flash`], rendering each phase, then re-read `dev.desc`.
fn flash_with_progress(device: &proxy::Port, firmware_data: &[u8]) -> eyre::Result<()> {
    let power_cycle_hint =
        "power cycle the device before retrying and check if dev.stop exists as an rpc";
    // Wide enough to align the longest RPC name (dev.firmware.upgrade).
    let label = |s: &str| style(format!("{:20}", s)).bold().cyan();

    // Bars are created lazily so the dev.stop lines print cleanly above them.
    let mut upload: Option<ProgressBar> = None;
    let mut finalize: Option<ProgressBar> = None;

    let result = firmware::flash(device, firmware_data, |event| match event {
        FlashEvent::Stopping => {
            println!(" {} stopping device...", label("dev.stop"));
        }
        FlashEvent::Stopped(outcome) => {
            let result = match outcome {
                StopOutcome::Stopped => style("stopped").green(),
                StopOutcome::AlreadyStopped => style("already stopped").green(),
                StopOutcome::Unsupported => style("not supported (continuing)").yellow(),
            };
            println!(" {} {}", label("dev.stop"), result);
        }
        FlashEvent::Uploading { chunk, total } => {
            let bar = upload.get_or_insert_with(|| {
                let b = crate::multi_progress().add(ProgressBar::new(total as u64));
                b.set_style(
                    ProgressStyle::default_bar()
                        .template(
                            "{spinner:.green} dev.firmware.upload [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)",
                        )
                        .unwrap()
                        .progress_chars("#>-"),
                );
                b
            });
            bar.set_length(total as u64);
            bar.set_position(chunk as u64);
        }
        FlashEvent::Committing => {
            // Persist the upload result (the bar above is transient).
            if let Some(bar) = upload.take() {
                let chunks = bar.length().unwrap_or(0);
                bar.finish_and_clear();
                println!(" {} {} chunks uploaded", label("dev.firmware.upload"), chunks);
            }
            println!(" {} committing...", label("dev.firmware.upgrade"));
        }
        FlashEvent::Finalizing => {
            let spinner = crate::multi_progress().add(ProgressBar::new_spinner());
            spinner.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap(),
            );
            spinner.set_message("Finalizing upgrade...");
            spinner.enable_steady_tick(Duration::from_millis(100));
            finalize = Some(spinner);
        }
        FlashEvent::Complete => {
            if let Some(spinner) = finalize.take() {
                spinner.finish_and_clear();
            }
        }
    });

    if let Some(bar) = upload.take() {
        bar.finish_and_clear();
    }
    if let Some(spinner) = finalize.take() {
        spinner.finish_and_clear();
    }

    result
        .wrap_err("firmware upgrade failed")
        .suggestion(power_cycle_hint)?;

    println!(" {} {}", label("dev.firmware.upgrade"), style("complete").green());

    verify_new_firmware(device);
    Ok(())
}

/// After a successful flash, re-read `dev.desc` (retrying while the device
/// reboots) to confirm the new firmware booted. Best-effort: never fails.
fn verify_new_firmware(device: &proxy::Port) {
    const ATTEMPTS: usize = 6;
    const INTERVAL: Duration = Duration::from_millis(1500);

    let spinner = crate::multi_progress().add(ProgressBar::new_spinner());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message("Verifying new firmware (re-reading dev.desc)...");
    spinner.enable_steady_tick(Duration::from_millis(100));

    let mut installed = None;
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(INTERVAL);
        }
        if let Ok(fw) = firmware::query_installed(device) {
            installed = Some(fw);
            break;
        }
    }
    spinner.finish_and_clear();

    let label = |s: &str| style(format!("{:11}", s)).bold().cyan();
    match installed {
        Some(fw) => println!(" {} {}", label("Running:"), fw.description),
        None => println!(
            "{}",
            style("Could not re-read dev.desc after upgrade (device may still be rebooting).")
                .yellow()
        ),
    }
}

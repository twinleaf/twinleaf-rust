use clap::{Args, ValueHint};
use std::path::PathBuf;

use crate::{parse_existing_file, TioOpts};

#[derive(Args, Debug)]
pub struct UpgradeCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Input firmware image path. Omit to download the latest published
    /// firmware for the connected sensor's name and hardware revision.
    #[arg(value_hint = ValueHint::FilePath, value_parser = parse_existing_file)]
    pub firmware_path: Option<PathBuf>,

    /// List all published firmware for the connected sensor and interactively
    /// pick one to install (allows installing an older release).
    #[arg(long, conflicts_with = "firmware_path")]
    pub downgrade: bool,

    /// Skip confirmation prompt
    #[arg(short = 'y', long = "yes")]
    pub yes: bool,
}

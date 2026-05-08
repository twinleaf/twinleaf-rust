use clap::{Args, ValueHint};
use std::path::PathBuf;

use crate::{parse_existing_file, TioOpts};

#[derive(Args, Debug)]
pub struct UpgradeCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Input firmware image path
    #[arg(value_hint = ValueHint::FilePath, value_parser = parse_existing_file)]
    pub firmware_path: PathBuf,

    /// Skip confirmation prompt
    #[arg(short = 'y', long = "yes")]
    pub yes: bool,
}

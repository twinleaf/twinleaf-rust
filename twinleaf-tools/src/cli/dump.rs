use clap::Args;
use std::time::Duration;

use crate::TioOpts;

#[derive(Args, Debug)]
pub struct DumpCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Show parsed data samples
    #[arg(short = 'd', long = "data")]
    pub data: bool,

    /// Show metadata on boundaries
    #[arg(short = 'm', long = "meta")]
    pub meta: bool,

    /// Routing depth limit (default: unlimited)
    #[arg(long = "depth")]
    pub depth: Option<usize>,

    /// Stop after this wall-clock duration (e.g. 30s, 5m, 2h)
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,
}

use clap::Args;

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
}

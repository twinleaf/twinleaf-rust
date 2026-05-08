use clap::Parser;

use crate::TioOpts;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Live sensor data display")]
pub struct MonitorCli {
    #[command(flatten)]
    pub(crate) tio: TioOpts,

    #[arg(long = "fps", default_value_t = 20)]
    pub(crate) fps: u32,

    #[arg(short = 'c', long = "colors")]
    pub(crate) colors: Option<String>,

    /// Routing depth limit (default: unlimited)
    #[arg(long = "depth")]
    pub(crate) depth: Option<usize>,
}

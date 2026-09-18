use clap::Parser;

use crate::TioOpts;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Live sensor data display")]
pub struct MonitorCli {
    #[command(flatten)]
    pub(crate) tio: TioOpts,

    /// UI refresh rate
    #[arg(
        long = "fps",
        default_value = "20",
        value_name = "FPS",
        value_parser = clap::value_parser!(u32).range(1..=60),
        help = "UI refresh rate in frames per second (1-60)"
    )]
    pub(crate) fps: u32,

    /// TOML file coloring columns by value
    #[arg(
        short = 'c',
        long = "colors",
        value_name = "FILE",
        help = "TOML file of per-column value bounds: `stream.column = { cold = .., hot = .. }` or `{ min = .., max = .. }`"
    )]
    pub(crate) colors: Option<String>,

    /// Routing depth limit (default: unlimited)
    #[arg(long = "depth")]
    pub(crate) depth: Option<usize>,
}

#[derive(Parser, Debug)]
#[command(name = "tio-monitor", version, about = "Display live sensor data")]
pub struct MonitorCli {
    #[command(flatten)]
    pub tio: TioOpts,
    #[arg(short = 'a', long = "all")]
    pub all: bool,
    #[arg(long = "fps", default_value_t = 20)]
    pub fps: u32,
    #[arg(short = 'c', long = "colors")]
    pub colors: Option<String>,
}

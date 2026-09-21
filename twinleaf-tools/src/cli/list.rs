use clap::Args;
use std::time::Duration;

#[derive(Args, Debug)]
pub struct ListCli {
    /// Include serial ports with unknown VID/PID
    #[arg(short = 'a', long = "all")]
    pub all: bool,

    /// Only search local serial ports; skip mDNS network discovery
    #[arg(long = "local")]
    pub local: bool,

    /// Prefer UDP when a network device offers both TCP and UDP
    #[arg(long = "udp")]
    pub udp: bool,

    /// How long to browse before printing when output isn't a terminal (e.g. 2s, 500ms)
    #[arg(long = "duration", default_value = "2s", value_parser = humantime::parse_duration)]
    pub duration: Duration,
}

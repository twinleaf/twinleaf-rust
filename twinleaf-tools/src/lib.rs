use tio::proto::DeviceRoute;
use tio::util;
use twinleaf::tio;
use clap::Parser;

include!("proxy_cli.rs");
include!("tool_cli.rs");
include!("monitor_cli.rs");
include!("health_cli.rs");
#[derive(Parser, Debug, Clone)]
pub struct TioOpts {
    /// Sensor root address (e.g., tcp://localhost, serial:///dev/ttyUSB0)
    #[arg(
        short = 'r',
        long = "root",
        default_value_t = util::default_proxy_url().to_string(),
        help = "Sensor root address"
    )]
    pub root: String,

    /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
    #[arg(
        short = 's',
        long = "sensor",
        default_value = "/",
        help = "Sensor path in the sensor tree"
    )]
    pub route_path: String,
}

impl TioOpts {
    pub fn parse_route(&self) -> DeviceRoute {
        DeviceRoute::from_str(&self.route_path).unwrap_or_else(|_| DeviceRoute::root())
    }
}

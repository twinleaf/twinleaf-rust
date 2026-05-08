use clap::Parser;
use std::path::PathBuf;
use tio::proto::DeviceRoute;
use tio::util;
use twinleaf::tio;
pub mod cli;
pub mod tools;
pub mod tui;

pub use cli::*;

pub fn install_error_handler() -> eyre::Result<()> {
    color_eyre::config::HookBuilder::default()
        .display_env_section(false)
        .display_location_section(false)
        .install()
}

fn parse_device_route(s: &str) -> Result<DeviceRoute, String> {
    DeviceRoute::from_str(s).map_err(|_| format!("invalid sensor route: {s:?}"))
}

fn parse_existing_file(s: &str) -> Result<PathBuf, String> {
    let p = PathBuf::from(s);
    match std::fs::metadata(&p) {
        Ok(m) if m.is_file() => Ok(p),
        Ok(_) => Err(format!("not a regular file: {s:?}")),
        Err(e) => Err(format!("cannot read {s:?}: {e}")),
    }
}

#[derive(Parser, Debug, Clone)]
pub struct TioOpts {
    /// Sensor root address (e.g., tcp://localhost, serial:///dev/ttyUSB0)
    #[arg(
        short = 'r',
        long = "root",
        default_value_t = util::default_proxy_url().to_string(),
        value_hint = clap::ValueHint::Url,
        help = "Sensor root address"
    )]
    pub root: String,

    /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
    #[arg(
        short = 's',
        long = "sensor",
        default_value = "/",
        value_parser = parse_device_route,
        help = "Sensor path in the sensor tree"
    )]
    pub route: DeviceRoute,
}

use clap::Parser;
use indicatif::MultiProgress;
use std::path::PathBuf;
use std::sync::OnceLock;
use twinleaf::device::DeviceRoute;
use twinleaf::tio::util;
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

static MULTI_PROGRESS: OnceLock<MultiProgress> = OnceLock::new();

pub fn init_logging() {
    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_target(false)
            .build();
    let max_level = logger.filter();
    let mp = MultiProgress::new();
    indicatif_log_bridge::LogWrapper::new(mp.clone(), logger)
        .try_init()
        .expect("logger already initialized");
    log::set_max_level(max_level);
    let _ = MULTI_PROGRESS.set(mp);
}

pub fn multi_progress() -> &'static MultiProgress {
    MULTI_PROGRESS
        .get()
        .expect("init_logging() must be called before multi_progress()")
}

pub trait ProxyHelp<T> {
    fn with_proxy_help(self) -> eyre::Result<T>;
}

impl<T> ProxyHelp<T> for eyre::Result<T> {
    fn with_proxy_help(self) -> eyre::Result<T> {
        use color_eyre::Help;
        self.suggestion("start `tio proxy` first if using with multiple applications")
            .suggestion("or specify a source with -r <url>")
    }
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

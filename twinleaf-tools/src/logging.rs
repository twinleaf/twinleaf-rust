//! The logger each subcommand writes through. [`init`] chooses it before the
//! subcommand runs.

use crate::{Commands, ProxyCli, ProxySubcommands};
use chrono::Local;
use indicatif::MultiProgress;
use log::{Level, LevelFilter, Log, Metadata, Record};
use std::io::IsTerminal;
use std::sync::{Mutex, OnceLock};

/// Installs the logger that `command` writes through.
///
/// The plain tools write records on stderr. `tio proxy` and its subcommands
/// write them in the proxy's own format. `tio list` on a terminal writes
/// nothing while its picker holds the screen, and writes as the proxy does
/// once [`release`] is called. Health keeps its latest record for its screen,
/// where [`latest`] reads it. Monitor logs nothing.
///
/// # Panics
///
/// Panics if a logger is already installed.
pub fn init(command: &Commands) {
    match command {
        Commands::Monitor(_) => {}
        Commands::Health(_) => screen(),
        Commands::List(_)
        | Commands::Proxy(ProxyCli {
            subcommands: Some(ProxySubcommands::List(_)),
            ..
        }) => {
            if std::io::stdout().is_terminal() {
                proxy(false, false, false);
                let _ = HELD.set(log::max_level());
                log::set_max_level(LevelFilter::Off);
            } else {
                plain();
            }
        }
        Commands::Proxy(ProxyCli {
            subcommands: None,
            verbose,
            debug,
            background,
            ..
        }) => proxy(*verbose, *debug, *background),
        Commands::Proxy(_) => proxy(false, false, false),
        _ => plain(),
    }
}

/// The level that `tio list` logs at once its picker closes.
static HELD: OnceLock<LevelFilter> = OnceLock::new();

/// Lets the logger that `tio list` holds back while its picker is open write.
pub fn release() {
    if let Some(level) = HELD.get() {
        log::set_max_level(*level);
    }
}

/// Returns the progress bars of the plain tools, which their log records
/// print around.
pub fn multi_progress() -> &'static MultiProgress {
    static MULTI_PROGRESS: OnceLock<MultiProgress> = OnceLock::new();
    MULTI_PROGRESS.get_or_init(MultiProgress::new)
}

/// Installs a logger that writes records at info and above, or as `RUST_LOG`
/// says, on stderr and around any progress bars.
///
/// # Panics
///
/// Panics if a logger is already installed.
pub fn plain() {
    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_target(false)
            .build();
    let max_level = logger.filter();
    indicatif_log_bridge::LogWrapper::new(multi_progress().clone(), logger)
        .try_init()
        .expect("logger already initialized");
    log::set_max_level(max_level);
}

/// Installs a logger that writes records in the proxy's format. A background
/// proxy logs its own lifecycle in full and leaves out device logs.
fn proxy(verbose: bool, debug: bool, background: bool) {
    use std::io::Write;
    let level_filter = if background {
        "info,twinleaf=debug,twinleaf_tools=debug,device=off"
    } else if debug {
        "trace"
    } else if verbose {
        "debug"
    } else {
        "info,device=debug"
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level_filter))
        .format(|buf, record| {
            let level = record.level();
            let level_style = buf.default_level_style(level);
            let target = record.target();
            let source = target
                .strip_prefix("device::")
                .unwrap_or_else(|| target.rsplit("::").next().unwrap_or(target));
            let bold = env_logger::fmt::style::Style::new().bold();
            let ts = chrono::Local::now().format("%T%.3f");
            writeln!(
                buf,
                "{ts} {level_style}{level:5}{level_style:#} {bold}{source}:{bold:#} {}",
                record.args()
            )
        })
        .init();
}

/// The latest record the screen logger kept, with its level, as a line of
/// its time, level and message.
static LATEST: Mutex<Option<(Level, String)>> = Mutex::new(None);

/// Returns the latest record at warn or above, or at the level `RUST_LOG`
/// sets, for a full screen tool to show.
pub fn latest() -> Option<(Level, String)> {
    let latest = LATEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    latest.clone()
}

/// Installs a logger that keeps the latest record at warn or above, or as
/// `RUST_LOG` says, for [`latest`] to read.
fn screen() {
    let filter =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).build();
    let max_level = filter.filter();
    log::set_boxed_logger(Box::new(ScreenLogger { filter })).expect("logger already initialized");
    log::set_max_level(max_level);
}

/// The logger [`screen`] installs. Its `env_logger` only filters.
struct ScreenLogger {
    filter: env_logger::Logger,
}

impl Log for ScreenLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.filter.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if !self.filter.matches(record) {
            return;
        }
        let line = format!(
            "{} {} {}",
            Local::now().format("%H:%M:%S"),
            record.level(),
            record.args()
        );
        let mut latest = LATEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *latest = Some((record.level(), line));
    }

    fn flush(&self) {}
}

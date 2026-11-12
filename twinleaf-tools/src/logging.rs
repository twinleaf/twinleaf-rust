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
                proxy(false, false, false, None);
                let _ = HELD.set(log::max_level());
                log::set_max_level(LevelFilter::Off);
            } else {
                plain();
            }
        }
        Commands::Proxy(
            cli @ ProxyCli {
                subcommands: None,
                verbose,
                debug,
                background,
                ..
            },
        ) => proxy(*verbose, *debug, *background, page_log(cli)),
        Commands::Proxy(_) => proxy(false, false, false, None),
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

/// What takes each record the proxy's console prints, with its level, its
/// source as the console names it, and its text.
type Sink = Box<dyn Fn(Level, &str, &str) + Send + Sync>;

/// The log of the web page, which takes what the console of a `tio proxy`
/// that serves the page prints, or `None` for a proxy that serves none.
fn page_log(cli: &ProxyCli) -> Option<Sink> {
    #[cfg(feature = "web")]
    return crate::tools::proxy::serves_page(cli).then(|| Box::new(twinleaf_web::console) as Sink);
    #[cfg(not(feature = "web"))]
    {
        let _ = cli;
        None
    }
}

/// Installs a logger that writes records in the proxy's format, and hands
/// each record it writes to `sink`. A background proxy logs its own
/// lifecycle in full and leaves out device logs.
fn proxy(verbose: bool, debug: bool, background: bool, sink: Option<Sink>) {
    let level_filter = if background {
        "info,twinleaf=debug,twinleaf_tools=debug,device=off"
    } else if debug {
        "trace"
    } else if verbose {
        "debug"
    } else {
        "info,device=debug"
    };
    let builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level_filter));
    let logger = proxy_logger(builder, sink);
    let max_level = logger.console.filter();
    log::set_boxed_logger(Box::new(logger)).expect("logger already initialized");
    log::set_max_level(max_level);
}

/// The source the proxy's console names a record of `target` by. A device's
/// route for a device's own record, and else the last part of the target.
fn source_of(target: &str) -> &str {
    target
        .strip_prefix(DEVICE)
        .unwrap_or_else(|| target.rsplit("::").next().unwrap_or(target))
}

/// What the target of a device's own log record starts with.
const DEVICE: &str = "device::";

/// The proxy's logger over `builder`, which says what it writes and where.
/// It writes each record in the proxy's format, and hands `sink` each
/// record it writes but a device's own, which the page's hub reads from the
/// device's log packets.
fn proxy_logger(mut builder: env_logger::Builder, sink: Option<Sink>) -> ProxyLogger {
    use std::io::Write;
    let console = builder
        .format(|buf, record| {
            let level = record.level();
            let level_style = buf.default_level_style(level);
            let source = source_of(record.target());
            let bold = env_logger::fmt::style::Style::new().bold();
            let ts = chrono::Local::now().format("%T%.3f");
            writeln!(
                buf,
                "{ts} {level_style}{level:5}{level_style:#} {bold}{source}:{bold:#} {}",
                record.args()
            )
        })
        .build();
    ProxyLogger { console, sink }
}

/// The logger [`proxy`] installs. Its `env_logger` filters and writes.
struct ProxyLogger {
    console: env_logger::Logger,
    sink: Option<Sink>,
}

impl Log for ProxyLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.console.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if !self.console.matches(record) {
            return;
        }
        self.console.log(record);
        let target = record.target();
        if let Some(sink) = self.sink.as_ref().filter(|_| !target.starts_with(DEVICE)) {
            sink(
                record.level(),
                source_of(target),
                &record.args().to_string(),
            );
        }
    }

    fn flush(&self) {
        self.console.flush();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// What a logger wrote, shared with the test that reads it.
    #[derive(Clone, Default)]
    struct Written(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Written {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A builder of the proxy's console at its usual filter, writing into
    /// `written` with the styles a terminal gets.
    fn console(written: &Written) -> env_logger::Builder {
        let mut builder = env_logger::Builder::new();
        builder
            .parse_filters("info,device=debug")
            .write_style(env_logger::WriteStyle::Always)
            .target(env_logger::Target::Pipe(Box::new(written.clone())));
        builder
    }

    /// Logs each of `records`, a level, a target and a text, through
    /// `logger`.
    fn log_all(logger: &dyn Log, records: &[(Level, &str, &str)]) {
        for (level, target, text) in records {
            logger.log(
                &Record::builder()
                    .level(*level)
                    .target(target)
                    .args(format_args!("{text}"))
                    .build(),
            );
        }
    }

    /// The lines `written` holds, each without its time of day.
    fn lines(written: &Written) -> Vec<String> {
        let text = String::from_utf8(written.0.lock().unwrap().clone()).unwrap();
        text.lines().map(|line| line[12..].to_string()).collect()
    }

    const RECORDS: [(Level, &str, &str); 5] = [
        (Level::Warn, "proxy::udp://127.0.0.1", "Sensor disconnected"),
        (Level::Info, "twinleaf::proxy::server", "client 3 ended"),
        (Level::Debug, "twinleaf::proxy::server", "under the filter"),
        (Level::Debug, "device::/0", "a device's own line"),
        (Level::Error, "plain", "no path"),
    ];

    /// The console writes the same bytes with a sink as with none, each
    /// record the filter passes as its time, its level, its source in bold
    /// and its text.
    #[test]
    fn the_console_writes_the_same_bytes_with_a_sink_as_without() {
        let (alone, with) = (Written::default(), Written::default());
        log_all(&proxy_logger(console(&alone), None), &RECORDS);
        let sink: Sink = Box::new(|_, _, _| {});
        log_all(&proxy_logger(console(&with), Some(sink)), &RECORDS);
        assert_eq!(lines(&with), lines(&alone));
        let bold = env_logger::fmt::style::Style::new().bold();
        assert_eq!(lines(&alone).len(), 4);
        assert!(
            lines(&alone)[0].ends_with(&format!(
                " {bold}udp://127.0.0.1:{bold:#} Sensor disconnected"
            )),
            "{:?}",
            lines(&alone)[0]
        );
        assert!(lines(&alone)[0].contains("WARN "));
        assert!(lines(&alone)[2].ends_with(&format!(" {bold}/0:{bold:#} a device's own line")));
        let whole = String::from_utf8(alone.0.lock().unwrap().clone()).unwrap();
        let time = &whole[..12];
        assert!(
            time.as_bytes()[2] == b':' && time.as_bytes()[8] == b'.',
            "{time}"
        );
    }

    /// The sink takes each record the console writes, with its level, the
    /// source the console names and its text, in order. It takes none the
    /// filter stops, and none of a device's own, which the console writes.
    #[test]
    fn the_sink_takes_what_the_console_writes_but_a_devices_own_records() {
        let taken: Arc<Mutex<Vec<(Level, String, String)>>> = Arc::default();
        let into = taken.clone();
        let sink: Sink = Box::new(move |level, source, text| {
            let record = (level, source.to_string(), text.to_string());
            into.lock().unwrap().push(record);
        });
        let written = Written::default();
        log_all(&proxy_logger(console(&written), Some(sink)), &RECORDS);
        let own = |level, source: &str, text: &str| (level, source.to_string(), text.to_string());
        assert_eq!(
            *taken.lock().unwrap(),
            [
                own(Level::Warn, "udp://127.0.0.1", "Sensor disconnected"),
                own(Level::Info, "server", "client 3 ended"),
                own(Level::Error, "plain", "no path"),
            ]
        );
        assert_eq!(lines(&written).len(), 4, "the console wrote the device's");
    }
}

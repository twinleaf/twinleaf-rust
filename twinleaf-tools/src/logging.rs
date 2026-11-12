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

/// What the target starts with of a record that says a link status the
/// proxy also announces to its clients.
const STATUS: &str = "status::";

/// What the target starts with of a record a link's worker logs of its
/// device, ahead of the link's URL.
const LINK: &str = "proxy::";

/// The module whose records are a link's worker's too.
const WORKER: &str = "twinleaf::proxy::worker";

/// The part of the proxy a link's records are said by, on the page.
const LINK_PART: &str = "link";

/// What the page's log takes of a record of `target` saying `text`, as the
/// part of the proxy that said it and its words, or `None` for a record
/// the page's hub hears through its own connection, a device's own log
/// record or a link status the proxy announces. Every record of a link is
/// said by the one part, its worker's with the link's URL ahead of the
/// words, and any other record by the source the console names.
fn for_page(target: &str, text: String) -> Option<(&str, String)> {
    if target.starts_with(DEVICE) || target.starts_with(STATUS) {
        return None;
    }
    Some(match target.strip_prefix(LINK) {
        Some(url) => (LINK_PART, format!("{url}: {text}")),
        None if target == WORKER => (LINK_PART, text),
        None => (source_of(target), text),
    })
}

/// The proxy's logger over `builder`, which says what it writes and where.
/// It writes each record in the proxy's format, and hands `sink` each
/// record it writes that is for the page ([`for_page`]).
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
        if let Some(sink) = &self.sink {
            if let Some((part, text)) = for_page(record.target(), record.args().to_string()) {
                sink(record.level(), part, &text);
            }
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

    const RECORDS: [(Level, &str, &str); 9] = [
        (
            Level::Warn,
            "status::udp://127.0.0.1",
            "Sensor disconnected",
        ),
        (Level::Info, "twinleaf::proxy::server", "client 3 ended"),
        (Level::Debug, "twinleaf::proxy::server", "under the filter"),
        (Level::Debug, "device::/0", "a device's own line"),
        (Level::Error, "plain", "no path"),
        (
            Level::Info,
            "twinleaf::proxy::link",
            "Opening udp://127.0.0.1",
        ),
        (Level::Info, "proxy::udp://127.0.0.1", "Text: hello"),
        (
            Level::Warn,
            "twinleaf::proxy::worker",
            "Failed to find RPC timeout in map",
        ),
        (Level::Info, "status::udp://127.0.0.1", "Sensor reconnected"),
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
        assert_eq!(lines(&alone).len(), 8);
        assert!(lines(&alone)[7].ends_with(&format!(
            " {bold}udp://127.0.0.1:{bold:#} Sensor reconnected"
        )));
        assert!(lines(&alone)[5].ends_with(&format!(" {bold}udp://127.0.0.1:{bold:#} Text: hello")));
        assert!(lines(&alone)[6].ends_with(&format!(
            " {bold}worker:{bold:#} Failed to find RPC timeout in map"
        )));
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
    /// part that said it and its text, in order. It takes none the filter
    /// stops, and none of a device's own or of a link status the proxy
    /// announces, which the console writes. A link's records are all said
    /// by the part link, its worker's with the link's URL ahead of them.
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
                own(Level::Info, "server", "client 3 ended"),
                own(Level::Error, "plain", "no path"),
                own(Level::Info, "link", "Opening udp://127.0.0.1"),
                own(Level::Info, "link", "udp://127.0.0.1: Text: hello"),
                own(Level::Warn, "link", "Failed to find RPC timeout in map"),
            ]
        );
        assert_eq!(lines(&written).len(), 8, "the console wrote them all");
    }
}

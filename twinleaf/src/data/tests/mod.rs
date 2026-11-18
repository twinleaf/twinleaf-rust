pub(crate) mod fixtures;

use std::cell::RefCell;

thread_local! {
    /// The warnings this thread logs while [`warnings_during`] runs.
    static WARNINGS: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
}

/// Keeps each warning a thread logs while that thread captures them.
struct Capture;

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        WARNINGS.with(|warnings| {
            if let Some(warnings) = warnings.borrow_mut().as_mut() {
                warnings.push(record.args().to_string());
            }
        });
    }

    fn flush(&self) {}
}

/// Runs `run` and returns the warnings it logged on this thread, in order.
pub(crate) fn warnings_during(run: impl FnOnce()) -> Vec<String> {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        if log::set_logger(&Capture).is_ok() {
            log::set_max_level(log::LevelFilter::Warn);
        }
    });
    WARNINGS.with(|warnings| *warnings.borrow_mut() = Some(Vec::new()));
    run();
    WARNINGS.with(|warnings| warnings.borrow_mut().take().unwrap_or_default())
}

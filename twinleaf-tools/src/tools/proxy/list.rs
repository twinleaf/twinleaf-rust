//! `tio list` — discover connected and networked Twinleaf devices, live.
//!
//! Streams devices from the shared discovery engine into the inline selector
//! view (see [`crate::tui::selector`]), then leaves a plain-text snapshot.

use std::time::Duration;

use twinleaf::device::discovery::DiscoveryConfig;

pub fn run_list(all: bool, local: bool, duration: Duration) -> eyre::Result<()> {
    let config = DiscoveryConfig {
        include_unknown: all,
        network: !local,
        probe_names: true,
    };
    crate::tui::selector::list_devices(config, duration)
}

/// Called from `tio proxy --enumerate` for backward compatibility; emits a
/// deprecation warning and delegates to `run_list`.
pub fn list_devices_deprecated(all: bool) -> eyre::Result<()> {
    eprintln!("warning: 'tio proxy --enumerate' is deprecated; use 'tio list' instead");
    run_list(all, false, Duration::from_secs(3))
}

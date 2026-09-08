//! Device discovery frontend for `tio proxy list` and its `tio list` shortcut.
//!
//! On a terminal, opens the interactive picker (see [`crate::tui::selector`]):
//! selecting devices hosts them as the default until Ctrl-C, quitting prints
//! the device tree.
//! Without a TTY, prints the tree after the scan window.

use std::io::IsTerminal;
use std::time::Duration;

use crate::ListCli;
use twinleaf::device::discovery::DiscoveryConfig;

pub fn run_list(cli: ListCli) -> eyre::Result<()> {
    let config = DiscoveryConfig {
        include_unknown: cli.all,
        network: !cli.local,
        probe_names: true,
        prefer_udp: cli.udp,
    };
    if !std::io::stdout().is_terminal() {
        crate::init_logging();
    }
    match crate::tui::selector::list_devices(config, cli.duration)? {
        Some(mounts) => super::run_proxy_for(mounts),
        None => Ok(()),
    }
}

/// Called from `tio proxy --enumerate` for backward compatibility; emits a
/// deprecation warning and prints the device tree (never the picker).
pub(super) fn list_devices_deprecated(all: bool) -> eyre::Result<()> {
    eprintln!("warning: 'tio proxy --enumerate' is deprecated; use 'tio proxy list' instead");
    let config = DiscoveryConfig {
        include_unknown: all,
        network: true,
        probe_names: true,
        prefer_udp: false,
    };
    crate::tui::selector::print_device_list(config, Duration::from_secs(3));
    Ok(())
}

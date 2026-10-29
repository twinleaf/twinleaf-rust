//! Device discovery frontend for `tio proxy list` and its `tio list` shortcut.
//!
//! On a terminal, [`run_list`] opens the interactive picker of
//! [`crate::tui::selector`], which discovers through the machine's proxy as
//! any tool does. Selecting devices makes them that proxy's default until
//! Ctrl-C. Quitting prints the device tree. Without a terminal, or with
//! `--json`, it prints the tree after the scan window.

use crate::ListCli;
use twinleaf::device::discovery::DiscoveryConfig;

pub fn run_list(cli: ListCli) -> eyre::Result<()> {
    let config = DiscoveryConfig {
        include_unknown: cli.all,
        network: !cli.local,
        probe_names: true,
        prefer_udp: cli.udp,
    };
    match crate::tui::selector::list_devices(config, cli.duration, cli.json)? {
        Some(picked) => super::hold_selection(picked),
        None => Ok(()),
    }
}

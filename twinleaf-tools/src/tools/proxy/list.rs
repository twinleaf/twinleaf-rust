//! Device discovery frontend for `tio proxy list` and its `tio list` shortcut.
//!
//! On a terminal, [`run_list`] hosts the proxy as `tio proxy` does, then opens
//! the interactive picker of [`crate::tui::selector`]. Selecting devices makes
//! them the proxy's default until Ctrl-C. Quitting prints the device tree and
//! stops the proxy. Without a terminal, it prints the tree after the scan
//! window and hosts nothing.

use std::io::IsTerminal;

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
        crate::tui::selector::list_devices(config, cli.duration)?;
        return Ok(());
    }
    // A proxy already running leaves the picker working, and only a
    // selection reports it.
    let proxy = super::host_for_list();
    match crate::tui::selector::list_devices(config, cli.duration)? {
        Some(mounts) => super::run_proxy_for(proxy?, mounts),
        None => {
            if let Ok(proxy) = proxy {
                proxy.shutdown();
            }
            Ok(())
        }
    }
}

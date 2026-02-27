use std::{env, io};
use clap::{CommandFactory};
use clap_complete::{generate_to, Shell};

include!("src/lib.rs");

fn main() -> Result<(), io::Error> {
    let outdir = match env::var_os("OUT_DIR") {
        None => return Ok(()),
        Some(outdir) => outdir,
    };

    let mut proxy_cmd = ProxyCli::command();
    let mut tool_cmd = TioToolCli::command();
    let mut monitor_cmd = MonitorCli::command();
    let mut health_cmd = HealthCli::command();
    for &shell in Shell::value_variants() {
        generate_to(shell, &mut proxy_cmd, "tio-proxy", &outdir)?;
        generate_to(shell, &mut tool_cmd, "tio-tool", &outdir)?;
        generate_to(shell, &mut monitor_cmd, "tio-monitor", &outdir)?;
        generate_to(shell, &mut health_cmd, "tio-health", &outdir)?;
    }

    Ok(())
}

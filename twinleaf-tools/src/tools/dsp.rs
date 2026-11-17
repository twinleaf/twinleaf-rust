//! The `tio dsp` subcommand, which checks, draws, runs and serves signal
//! graphs with [`run_dsp`]. The commands themselves are in `twinleaf-dsp`.

use crate::DspCli;
use twinleaf_dsp::Library;

/// Runs a `tio dsp` command for the public functions.
///
/// A command that fails has already said why on stderr, in terms of the
/// spec it was given. The process ends here with that command's status, so
/// that no second report follows the first.
pub fn run_dsp(cli: DspCli) -> eyre::Result<()> {
    match twinleaf_dsp::cli::run_command(&Library::standard(), cli) {
        0 => Ok(()),
        status => std::process::exit(i32::from(status)),
    }
}

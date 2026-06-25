use crate::{CompletionsCli, TioCli};
use clap::CommandFactory;

pub fn run_completions(completions_cli: CompletionsCli) -> eyre::Result<()> {
    let shell = completions_cli.shell;
    clap_complete::aot::generate(shell, &mut TioCli::command(), "tio", &mut std::io::stdout());
    Ok(())
}

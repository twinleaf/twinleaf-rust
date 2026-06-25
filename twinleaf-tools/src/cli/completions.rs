use clap::Parser;
use clap_complete::Shell;

#[derive(Parser, Debug)]
#[command(
    version,
    long_about = "\
Generate shell completions for tio.

For dynamic completions (live RPC names completed from the device, for all
shells below), add the matching line to your shell's config file:

  Bash (~/.bashrc):
    source <(COMPLETE=bash tio)

  Zsh (~/.zshrc):
    source <(COMPLETE=zsh tio)

  Fish (~/.config/fish/config.fish):
    COMPLETE=fish tio | source

  Elvish (~/.elvish/rc.elv):
    eval (COMPLETE=elvish tio | slurp)

  PowerShell ($PROFILE):
    $env:COMPLETE = \"powershell\"; tio | Out-String | Invoke-Expression; Remove-Item Env:\\COMPLETE

`tio completions <shell>` prints a static script (no live RPC names) as a
fallback for setups that can't use the above."
)]
pub struct CompletionsCli {
    /// Shell to output a static completion script for
    #[arg(value_enum)]
    pub shell: Shell,
}

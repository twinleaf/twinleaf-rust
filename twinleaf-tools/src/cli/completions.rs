use clap_complete::Shell;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, long_about = "\
Generate shell completions for tio.

Dynamic RPC name completions are currently supported for only Bash and Zsh

Add one of these lines to your shell's config file:

  Bash (~/.bashrc):
    eval \"$(tio completions bash)\"

  Zsh (~/.zshrc):
    eval \"$(tio completions zsh)\"

  Fish (~/.config/fish/config.fish):
    tio completions fish | source

  PowerShell ($PROFILE):
    tio completions powershell | Invoke-Expression")]
pub struct CompletionsCli {
    /// Shell to output completions for
    #[arg(value_enum)]
    pub shell: Shell,

    /// Generate static instead of dynamic completions
    #[arg(short = 's', long = "static")]
    pub r#static: bool,
}

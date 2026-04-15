use std::{
    env, 
    fs::{create_dir_all, OpenOptions}, 
    io::{BufRead, BufReader, Result, Write}, 
    path::PathBuf,
    process::Command
};
use clap::{CommandFactory, Parser};
use clap_complete::{generate_to, Shell};
use twinleaf_tools::TioCli;

#[derive(Parser, Debug)]
#[command(name = "install_cli",)]
struct Cli {
    #[arg(short, long)]
    all : bool
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportedShell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
}

fn which(cmd: &str) -> bool {
    which::which(cmd).is_ok()
}

fn cur_active_shell() -> Option<String> {
    env::var("SHELL").ok().and_then(|s| s.split('/').last().map(|x| x.to_string()))
}

fn detect_installed_shells() -> Vec<SupportedShell> {
    let mut shells = Vec::new();

    if which("bash") {
        shells.push(SupportedShell::Bash);
    }
    if which("zsh") {
        shells.push(SupportedShell::Zsh);
    }
    if which("fish") {
        shells.push(SupportedShell::Fish);
    }
    if which("pwsh") || which("powershell") {
        shells.push(SupportedShell::PowerShell);
    }

    shells
}

fn get_os() -> &'static str {
    env::consts::OS
}

fn bash_paths() -> (PathBuf, Option<PathBuf> ){
    let home = PathBuf::from(env::var("HOME").expect("HOME not set"));
    let mut rc = home.clone();
    rc.push(".bashrc");
    let os = get_os(); 

    if os == "macos" {
        let brew = PathBuf::from("/opt/homebrew/etc/bash_completion.d/");
        if brew.exists() {
            return (brew, Some(rc));
        }

        let intel = PathBuf::from("/usr/local/etc/bash_completion.d/");
        if intel.exists() {
            return (intel, Some(rc));
        }
    }

    let mut dir = home.clone();
    dir.push(".local/share/bash-completion.completions/");
    (dir, Some(rc))
}

fn zsh_paths() -> (PathBuf, Option<PathBuf>){
    let home = PathBuf::from(env::var("HOME").expect("HOME not set"));
    let mut dir = home.clone();
    dir.push(".zsh/completions/");
    let mut rc = home.clone();
    rc.push(".zshrc");
    (dir, Some(rc))
}

fn fish_paths() -> (PathBuf, Option<PathBuf>) {
    let mut home = PathBuf::from(env::var("HOME").expect("HOME not set"));
    home.push(".config/fish/completions/");
    (home, None)
}

fn powershell_paths() -> (PathBuf, Option<PathBuf>) {
    let output = Command::new("pwsh")
        .args(["-NoProfile", "-Command", "$PROFILE"])
        .output()
        .or_else(|_| {
            Command::new("powershell")
                .args(["-NoProfile", "-Command", "$PROFILE"])
                .output()
        });
    if let Ok(out) = output {
        if out.status.success() {
            let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !path.is_empty() {
                return (PathBuf::new(), Some(PathBuf::from(path)))
            }
        }
    }
    let mut fallback = PathBuf::from(env::var("HOME").unwrap());
    fallback.push("Documents/PowerShell/Microsoft.PowerShell_profile.ps1");
    (PathBuf::new(), Some(fallback))
}

fn update_config(rc_path: &PathBuf, marker: &str, lines: &[String]) -> Result<()> {
    let mut exists = String::new();

    if rc_path.exists() {
        let file = OpenOptions::new().read(true).open(rc_path)?;
        let reader = BufReader::new(file);
        for line in reader.lines().flatten(){
            exists.push_str(&line);
            exists.push('\n');
        }
    }

    if exists.contains(marker) {
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(rc_path)?;

    writeln!(file, "\n# >>> tio completions >>>")?;
    for l in lines {
        writeln!(file, "{}", l)?;
    }
    writeln!(file, "# <<<<<<")?;
    Ok(())
}

fn install_shell_scripts(shell: SupportedShell) -> Result<()> {
    let mut cmd = TioCli::command();

    match shell {
        SupportedShell::Bash => {
            let (dir, rc) = bash_paths();
            create_dir_all(&dir)?;
            generate_to(Shell::Bash, &mut cmd, "tio", &dir)?;
            if let Some(rc) = rc {
                update_config(
                    &rc, 
                    "tio completions", 
                    &[format!("source {}/tio.bash", dir.display())],
                )?;
            }
        }
        SupportedShell::Zsh => {
            let (dir, rc) = zsh_paths();
            create_dir_all(&dir)?;
            generate_to(Shell::Zsh, &mut cmd, "tio", &dir)?;
            
            if let Some(rc) = rc {
                update_config(
                    &rc, 
                    "tio completions", 
                    &[
                        format!("fpath=({} $fpath)", dir.display()),
                        "autoload -Uz compinit && compinit".to_string(),
                    ]
                )?;
            }
        }
        SupportedShell::Fish => {
            let (dir, _) = fish_paths();
            create_dir_all(&dir)?;
            generate_to(Shell::Fish, &mut cmd, "tio", &dir)?;
        }
        SupportedShell::PowerShell => {
            let (_, rc) = powershell_paths();
            if let Some(rc) = rc {
                let mut completions_dir = PathBuf::from(env::var("HOME").unwrap());
                completions_dir.push(".config/powershell/completions");
                create_dir_all(&completions_dir)?;
                generate_to(Shell::PowerShell, &mut cmd, "tio", &completions_dir)?;
                let script_path = completions_dir.join("_tio.ps1");
                update_config(
                    &rc,
                    "tio completions", 
                    &[format!(". \"{}\"", script_path.to_string_lossy())],
                )?;
            }
        }
    }
    Ok(())
}

fn target_shells(all: bool) -> Vec<SupportedShell> {
    let installed_shells = detect_installed_shells();

    if all {
        return installed_shells;
    }

    if let Some(shell) = cur_active_shell() {
        return match shell.as_str() {
            "bash" => vec![SupportedShell::Bash], 
            "zsh" => vec![SupportedShell::Zsh], 
            "fish" => vec![SupportedShell::Fish], 
            "pwsh" | "powershell" => vec![SupportedShell::PowerShell],
            _ => installed_shells,
        };
    }
    installed_shells
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let targets = target_shells(args.all);

    for shell in targets{
        install_shell_scripts(shell)?;
    }
    println!("Tab completions successfully installed");
    Ok(())
}

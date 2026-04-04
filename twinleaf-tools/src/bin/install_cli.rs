use std::{
    env, 
    io::{Write, Result, BufRead, BufReader}, 
    fs::{create_dir_all, OpenOptions}, 
    path::PathBuf
};
use clap::CommandFactory;
use clap_complete::{generate_to, Shell};
use twinleaf_tools::TioCli;

fn get_shell_path(shell:Shell) -> (PathBuf, Option<PathBuf>){
    let home = env::var("HOME").unwrap();
    let mut dir = PathBuf::from(&home);

    let config = match shell {
        Shell::Bash => {
            dir.push(".local/share/bash-completion/completions/");
            None
        }
        Shell::Zsh => {
            dir.push(".zsh/completions/");
            let mut rc = PathBuf::from(&home);
            rc.push(".zshrc");
            Some(rc)
        }
        Shell::Fish => {
            dir.push(".config/fish/completions/");
            None
        }
        Shell::PowerShell => {
            let mut dir = PathBuf::from(&home);
            dir.push("Documents/PowerShell/Modules/");
            Some(dir)
        }
        _ => None,
    };
    (dir, config)

}

fn write_zsh_config(completion_dir: &PathBuf, rc_path: &PathBuf) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&rc_path)?;
    
    let dir_to_string = completion_dir.to_string_lossy();
    let reader = BufReader::new(&file);
    let exists = reader
        .lines()
        .filter_map(Result::ok)
        .any(|line| line.contains(&*dir_to_string));

    if !exists {
        writeln!(&mut file, "autoload -Uz compinit && compinit")?;
        writeln!(&mut file,"fpath=({:?} $fpath)", completion_dir)?;
    } 
    Ok(())
}

fn main() -> Result<()> {
    let outdir = Shell::from_env().unwrap_or(Shell::Bash);
    let (dir, config_path) = get_shell_path(outdir);
    create_dir_all(&dir)?; //check if completion dir path exists else create it

    let mut tio_cmd = TioCli::command();
    generate_to(outdir, &mut tio_cmd, "tio", &dir)?; //clap_complete tab complete script generation

    if let Some(rc) = config_path {
        if let Shell::Zsh = outdir {
            write_zsh_config(&dir, &rc)?;
        }
    }
    println!("Tab completions successfully installed");
    Ok(())
}

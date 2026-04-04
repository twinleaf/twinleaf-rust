use std::{
    env, 
    io::{Write, Result, BufRead, BufReader}, 
    fs::{create_dir_all, OpenOptions}, 
    path::PathBuf
};
use clap::{CommandFactory, ValueEnum};
use clap_complete::{generate_to, Shell};
use twinleaf_tools::TioCli;

fn get_shell_path(shell:Shell) -> (PathBuf, Option<PathBuf>){
    let home = env::var("HOME").unwrap();
    let mut dir = PathBuf::from(&home);

    let config = match shell {
        Shell::Bash => {
            dir.push(".local/share/bash-completion/completions/");
            let mut rc = PathBuf::from(&home);
            rc.push(".bashrc");
            Some(rc)
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
            dir.push("Documents/PowerShell/Modules/");
            None
        }
        _ => None,
    };
    (dir, config)

}

fn write_config(completion_dir: &PathBuf, rc_path: &PathBuf, write_items: Vec<String>) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&rc_path)?;
    
    let reader = BufReader::new(&file);
    let exists = reader
        .lines()
        .filter_map(Result::ok)
        .any(|line| line == &*completion_dir.to_string_lossy());

    if !exists {
        for item in write_items {
            writeln!(&mut file, "{}", item)?;
        }
    } 
    Ok(())
}

fn main() -> Result<()> {
    for &shell in Shell::value_variants(){ 
        let (dir, config_path) = get_shell_path(shell);
        create_dir_all(&dir)?; //check if completion dir path exists else create it

        let mut tio_cmd = TioCli::command();
        if dir != env::var("HOME").unwrap() {
            generate_to(shell, &mut tio_cmd, "tio", &dir)?; //clap_complete tab complete script generation
        }

        if let Some(rc) = config_path {
            if let Shell::Zsh = shell {
                let config = vec![format!("fpath=({} $fpath)", dir.display()), format!("autoload -Uz compinit && compinit")];
                write_config(&dir, &rc, config)?;
            } else if let Shell::Bash = shell {
                let config = vec![format!("source {}tio.bash", dir.display())];
                write_config(&dir, &rc, config)?;
            }
        }
    }
    println!("Tab completions successfully installed");
    Ok(())
}

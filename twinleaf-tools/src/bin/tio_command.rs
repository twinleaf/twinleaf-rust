use std::{env, io::{Write, Error}, fs::{create_dir_all, OpenOptions}};
use clap::{CommandFactory, ValueEnum};
use clap_complete::{generate_to, Shell};
use twinleaf_tools::TioCli;

fn main() -> Result<(), Error> {
    let mut dir = env::home_dir().expect("");
    let mut fpath = env::home_dir().expect("");
    let outdir = Shell::from_env().unwrap_or(Shell::Bash);
    let _script_path = match outdir { 
        Shell::Bash => {
            dir.push(String::from("usr/share/bash-completion/completions/"));
            fpath.push(String::from(".bashrc"))

        }, 
        Shell::Fish => {
            dir.push(String::from(".config/fish/completions/"));
            fpath.push(String::from(".config/fish/config.fish"))
        },
        Shell::Zsh => {
            dir.push(String::from(".zsh/site-functions/"));
            fpath.push(String::from(".zshrc"))
        },
        _ => {eprintln!("Can not provide tab completion scripts for this shell")}
    };
    create_dir_all(&dir)?; //create dir if it doesn't exist 
    let file = OpenOptions::new().append(true).create(true).open(fpath);
    match file {
        Ok(mut file) => writeln!(&mut file,"fpath=({:?} $fpath)", dir)?, 
        Err(_) => {eprintln!("Error writing fpath to shell configs")}
    };

	let mut tio_cmd = TioCli::command();
    for &shell in Shell::value_variants() {
        generate_to(shell, &mut tio_cmd, "tio", &dir)?;
    }

    Ok(())
}

use std::{env, io};
use clap::{CommandFactory};
use clap_complete::{generate_to, Shell};

include!("src/lib.rs");

fn main() -> Result<(), io::Error> {
    let outdir = match env::var_os("OUT_DIR") {
        None => return Ok(()),
        Some(outdir) => outdir,
    };
	
	let mut tio_cmd = TioCli::command();
    for &shell in Shell::value_variants() {
        generate_to(shell, &mut tio_cmd, "tio", &outdir)?;
    }

    Ok(())
}

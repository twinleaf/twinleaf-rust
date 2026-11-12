//! Builds the page, `ui/dist`, for a release build of a checkout that has
//! none, so that `cargo install` from the repository serves the page.
//!
//! A debug build reads `ui/dist` as it is when asked, so this builds
//! nothing for one. A release build with no `ui/dist/index.html` runs
//! `npm ci` and `npm run build` in `ui`, which needs Node and npm. Where
//! they are absent or fail, the build goes on with a warning, and the page
//! says how to build it. A published crate carries `ui/dist` and no page
//! sources, and builds nothing here. `TWINLEAF_WEB_NO_UI_BUILD`, set to
//! anything, skips the page's build.

use std::env;
use std::path::Path;
use std::process::Command;

/// The variable that skips the page's build.
const SKIP: &str = "TWINLEAF_WEB_NO_UI_BUILD";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=ui/dist/index.html");
    println!("cargo:rerun-if-env-changed={SKIP}");

    let manifest = env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let ui = Path::new(&manifest).join("ui");
    let release = env::var("PROFILE").is_ok_and(|profile| profile == "release");
    let built = ui.join("dist").join("index.html").exists();
    let sources = ui.join("package.json").exists();
    if !release || built || !sources || env::var_os(SKIP).is_some() {
        return;
    }

    if !ui.join("node_modules").exists() {
        if let Err(why) = npm(&ui, &["ci", "--no-audit", "--no-fund"]) {
            return not_built(&why);
        }
    }
    if let Err(why) = npm(&ui, &["run", "build"]) {
        not_built(&why);
    }
}

/// Runs `npm` with `args` in `ui`, and says why it did not succeed.
fn npm(ui: &Path, args: &[&str]) -> Result<(), String> {
    let mut command = if cfg!(windows) {
        let mut command = Command::new("cmd");
        command.args(["/C", "npm"]);
        command
    } else {
        Command::new("npm")
    };
    let said = format!("npm {}", args.join(" "));
    match command.args(args).current_dir(ui).output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let errors = String::from_utf8_lossy(&output.stderr);
            let last = errors.lines().rev().find(|line| !line.trim().is_empty());
            Err(format!(
                "`{said}` failed ({}){}",
                output.status,
                last.map(|line| format!(", ending {line}"))
                    .unwrap_or_default()
            ))
        }
        Err(error) => Err(format!("`{said}` did not run ({error})")),
    }
}

/// Warns that the binary has no page, and how to give it one.
fn not_built(why: &str) {
    println!("cargo:warning=The web page of tio proxy is not built, as {why}.");
    println!(
        "cargo:warning=Install Node and npm and build again, or run `npm ci` and `npm run build` in twinleaf-web/ui."
    );
}

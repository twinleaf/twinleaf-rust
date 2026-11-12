//! Builds the page, `ui/dist`, for a release build of a checkout whose
//! built page is missing or older than its sources, so that `cargo install`
//! from the repository serves the page as the checkout has it.
//!
//! A debug build reads `ui/dist` as it is when asked, so this builds
//! nothing for one. A release build runs `npm run build` in `ui`, after
//! `npm ci` where `ui/node_modules` is missing, when `ui/dist/index.html`
//! is missing or any of the page's sources was written after it. That
//! needs Node and npm. Where they are absent or fail, the build goes on
//! with a warning, and the binary has the page as `ui/dist` was, or a page
//! that says how to build it. A published crate carries `ui/dist` and no
//! page sources, and builds nothing here. `TWINLEAF_WEB_NO_UI_BUILD`, set
//! to anything, skips the page's build.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::SystemTime;

/// The variable that skips the page's build.
const SKIP: &str = "TWINLEAF_WEB_NO_UI_BUILD";

/// What the page is built from, within `ui`.
const SOURCES: [&str; 6] = [
    "src",
    "public",
    "index.html",
    "package.json",
    "package-lock.json",
    "vite.config.js",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=ui/dist/index.html");
    println!("cargo:rerun-if-env-changed={SKIP}");

    let manifest = env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let ui = Path::new(&manifest).join("ui");
    let release = env::var("PROFILE").is_ok_and(|profile| profile == "release");
    if !release || !ui.join("package.json").exists() || env::var_os(SKIP).is_some() {
        return;
    }
    for source in SOURCES {
        println!("cargo:rerun-if-changed=ui/{source}");
    }

    let built = written(&ui.join("dist").join("index.html"));
    let newest = SOURCES.iter().filter_map(|source| newest(&ui.join(source)));
    if built.is_some_and(|built| newest.max().is_none_or(|source| source <= built)) {
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

/// When the file at `path` was last written, if it is there.
fn written(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

/// When the newest file at or under `path` was last written, if any is
/// there.
fn newest(path: &Path) -> Option<SystemTime> {
    match fs::read_dir(path) {
        Ok(entries) => entries
            .filter_map(|entry| newest(&entry.ok()?.path()))
            .max(),
        Err(_) => written(path),
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

/// Warns that the page was not built, and how to build it.
fn not_built(why: &str) {
    println!("cargo:warning=The web page of tio proxy was not built, as {why}.");
    println!(
        "cargo:warning=Install Node and npm and build again, or run `npm ci` and `npm run build` in twinleaf-web/ui."
    );
}

//! Runs `tio dsp` as a user would, on the specs `twinleaf-dsp` keeps.
//!
//! What each command makes of a spec is tested in `twinleaf-dsp`. These
//! tests check that `tio` hands the command line over whole, and that it
//! ends with the command's own status.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Returns the path of a file of the `twinleaf-dsp` crate.
fn dsp_file(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../twinleaf-dsp")
        .join(path)
}

/// Runs `tio dsp` with `args` in `directory`. Returns its exit status and
/// what it wrote on stdout and stderr.
fn tio_dsp(args: &[&str], directory: &Path) -> (Option<i32>, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_tio"))
        .arg("dsp")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("tio runs");
    (
        output.status.code(),
        String::from_utf8(output.stdout).expect("stdout is text"),
        String::from_utf8(output.stderr).expect("stderr is text"),
    )
}

#[test]
fn tio_dsp_ends_with_the_status_of_its_command() {
    let examples = dsp_file("examples");
    let diagnostics = dsp_file("tests/diagnostics");
    if !examples.is_dir() {
        // The crate was packaged on its own, without its neighbor's specs.
        return;
    }

    let (code, stdout, stderr) = tio_dsp(&["check", "gradiometer.tsl"], &examples);
    assert_eq!((code, stderr.as_str()), (Some(0), ""));
    assert!(stdout.starts_with("gradiometer.tsl: ok\n"), "{stdout}");
    assert!(stdout.contains("out    gradient   /255/gradient  3 ch (x, y, z), nT, 500 Hz"));

    let (code, stdout, _) = tio_dsp(&["graph", "gradiometer.tsl", "--dot"], &examples);
    assert_eq!(code, Some(0));
    assert!(stdout.starts_with("digraph \"gradiometer\" {"), "{stdout}");

    let (code, stdout, _) = tio_dsp(&["functions"], &examples);
    assert_eq!(code, Some(0));
    assert!(stdout.contains("  decimate(n)\n"), "{stdout}");

    // A spec with errors is reported in its own terms, and nothing is added.
    let (code, stdout, stderr) = tio_dsp(&["check", "align_rates.tsl"], &diagnostics);
    assert_eq!((code, stdout.as_str()), (Some(1), ""));
    let rendered = fs::read_to_string(diagnostics.join("align_rates.stderr"))
        .expect("the rendered diagnostics are kept beside the spec");
    assert_eq!(
        stderr,
        format!("{rendered}\nerror: align_rates.tsl does not check: 2 errors\n")
    );

    // Warnings alone do not fail.
    let (code, _, stderr) = tio_dsp(&["check", "unused.tsl"], &diagnostics);
    assert_eq!(code, Some(0));
    assert!(
        stderr.ends_with("warning: unused.tsl: 3 warnings\n"),
        "{stderr}"
    );

    // A file that cannot be read and a wrong command line are a different
    // failure, which clap gives the same status.
    let (code, _, stderr) = tio_dsp(&["check", "absent.tsl"], &examples);
    assert_eq!(code, Some(2));
    assert!(
        stderr.starts_with("tio dsp: cannot read absent.tsl"),
        "{stderr}"
    );
    assert_eq!(tio_dsp(&["frobnicate"], &examples).0, Some(2));
    assert_eq!(tio_dsp(&["serve", "gradiometer.tsl"], &examples).0, Some(2));
    assert_eq!(tio_dsp(&["run", "gradiometer.tsl"], &examples).0, Some(2));

    let (code, stdout, _) = tio_dsp(&["--help"], &examples);
    assert_eq!(code, Some(0));
    assert!(stdout.contains("Usage: tio dsp <COMMAND>"), "{stdout}");
}

#[test]
fn completions_know_the_dsp_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_tio"))
        .args(["completions", "bash"])
        .output()
        .expect("tio runs");
    let script = String::from_utf8(output.stdout).expect("the script is text");
    assert!(
        script
            .lines()
            .any(|line| line.contains("dsp") && line.contains("serve")),
        "the script completes nothing under `tio dsp`"
    );
}

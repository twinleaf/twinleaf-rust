//! The rendered text of every diagnostic, checked in.
//!
//! Each `tests/diagnostics/NAME.tsl` is checked and its diagnostics rendered,
//! and the result must equal `NAME.stderr` beside it. Error messages are the
//! main interface to a language, and they rot silently otherwise.
//!
//! To accept changed output, run the test with `DSP_BLESS=1` and read the
//! diff.

use std::fs;
use std::path::PathBuf;
use twinleaf_dsp::lang::{self, diag};

fn directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/diagnostics")
}

/// Every spec in the directory, with its rendered diagnostics.
fn rendered() -> Vec<(PathBuf, String)> {
    let mut specs: Vec<PathBuf> = fs::read_dir(directory())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "tsl"))
        .collect();
    specs.sort();
    specs
        .into_iter()
        .map(|spec| {
            let source = fs::read_to_string(&spec).unwrap();
            let name = spec.file_name().unwrap().to_str().unwrap().to_owned();
            let analysis = lang::analyze(&source);
            let text = diag::render_all(&analysis.diagnostics, &name, &source);
            (spec.with_extension("stderr"), text)
        })
        .collect()
}

#[test]
fn rendered_diagnostics_match_their_snapshots() {
    let bless = std::env::var_os("DSP_BLESS").is_some();
    let mut stale = Vec::new();
    for (snapshot, text) in rendered() {
        if bless {
            fs::write(&snapshot, &text).unwrap();
        } else if fs::read_to_string(&snapshot).ok().as_deref() != Some(text.as_str()) {
            stale.push(format!(
                "--- {} now renders as ---\n{text}",
                snapshot.display()
            ));
        }
    }
    assert!(
        stale.is_empty(),
        "{}\nrun with DSP_BLESS=1 to accept",
        stale.join("\n")
    );
}

#[test]
fn every_code_has_a_snapshot() {
    // Diagnostics raised while running a recording are rendered by
    // tests/run.rs into tests/snapshots.
    let mut all = String::new();
    for directory in ["tests/diagnostics", "tests/snapshots"] {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(directory);
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "stderr") {
                all.push_str(&fs::read_to_string(path).unwrap());
            }
        }
    }
    for (code, meaning) in diag::code::ALL {
        assert!(
            all.contains(&format!("[{code}]")),
            "no snapshot shows {code}, {meaning}"
        );
    }
}

#[test]
fn codes_are_unique() {
    let mut codes: Vec<&str> = diag::code::ALL.iter().map(|(code, _)| *code).collect();
    codes.sort_unstable();
    let count = codes.len();
    codes.dedup();
    assert_eq!(codes.len(), count);
}

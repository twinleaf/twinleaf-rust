//! Checks that each fact about packet bytes has one home in the workspace.
//!
//! The test reads the workspace's Rust sources as text. Each rule names a
//! pattern and the path prefixes allowed to contain it. A source outside
//! those prefixes that contains the pattern fails the test, which names the
//! file and the line. A pattern that opens with a letter matches only where
//! a name starts, so `Header::parse(` does not match `PackageHeader::parse(`.
//! A pattern that opens with any other character matches anywhere.

use std::fs;
use std::path::{Path, PathBuf};

/// The source folders the rules read, from the workspace root.
const ROOTS: [&str; 6] = [
    "twinleaf/src",
    "twinleaf-tools/src",
    "twinleaf-tools/benches",
    "twinleaf-web/src",
    "twinleaf-proto/src",
    "twinleaf-device/src",
];

/// A pattern and the path prefixes, from the workspace root, that may hold
/// it.
struct Rule {
    patterns: &'static [&'static str],
    allowed: &'static [&'static str],
}

/// The rules. Each pattern decides a fact that only the allowed places own.
const RULES: &[Rule] = &[
    // Framing a packet from bytes, which the checked packet owns.
    Rule {
        patterns: &[
            "PacketView::parse_prefix",
            "Header::parse_prefix",
            "Header::parse(",
        ],
        allowed: &[
            "twinleaf-proto/",
            "twinleaf-device/",
            "twinleaf-tools/src/tools/simulate/",
            "twinleaf/src/tio/transport/codec/",
            "twinleaf/src/proxy/engine/tests/settings_sim.rs",
        ],
    },
    // Framing exact bytes without reading the payload, for an endpoint that
    // forwards packets or a packet whose bytes were checked when it was made.
    Rule {
        patterns: &["PacketView::parse_exact"],
        allowed: &[
            "twinleaf-proto/",
            "twinleaf-device/",
            "twinleaf-tools/src/tools/simulate/",
            "twinleaf/src/tio/packet.rs",
            "twinleaf/src/tio/transport/codec/",
        ],
    },
    // Writing a header by hand, which the whole-packet writers own.
    Rule {
        patterns: &["Header::new("],
        allowed: &["twinleaf-proto/", "twinleaf-device/"],
    },
    // Deciding what a sample packet is, which `PacketType::sample_stream`
    // owns.
    Rule {
        patterns: &["FIRST_STREAM_ID", "stream_id().is_some()"],
        allowed: &["twinleaf-proto/", "twinleaf-device/"],
    },
    // The text for a recorded file that does not decode.
    Rule {
        patterns: &["could not decode packet at byte offset"],
        allowed: &["twinleaf/src/data/logfile/reader.rs"],
    },
    // The text for a recorded packet the data state refuses.
    Rule {
        patterns: &["invalid data at byte offset"],
        allowed: &["twinleaf/src/data/logfile/reader.rs"],
    },
    // A capture selector as a bare argument value, which `capture::Selector`
    // owns.
    Rule {
        patterns: &["_INDEX: i16 = -"],
        allowed: &[],
    },
    // The code of a capture status, which `capture::Status` owns.
    Rule {
        patterns: &["4 => Self::Error"],
        allowed: &["twinleaf-proto/"],
    },
    // The size of a package header and of an image chunk, which the update
    // payloads own.
    Rule {
        patterns: &[": usize = 288", "[0u8; 288]", "[0; 288]"],
        allowed: &["twinleaf-proto/"],
    },
    // The envelope of an image chunk, which the update payloads own.
    Rule {
        patterns: &["_ENVELOPE: usize = 32"],
        allowed: &["twinleaf-proto/"],
    },
];

#[test]
fn each_packet_fact_has_one_home() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.join("..");
    let mut files = Vec::new();
    for root in ROOTS {
        collect_sources(&workspace.join(root), &mut files);
    }
    assert!(
        !files.is_empty(),
        "found no sources under {}",
        workspace.display()
    );
    files.sort();

    let mut sources = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).expect("a readable source file");
        let shown = file
            .strip_prefix(&workspace)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        sources.push((shown, text));
    }
    let findings = violations(RULES, &sources);
    assert!(
        findings.is_empty(),
        "a packet fact is decided outside its home\n{}",
        findings.join("\n")
    );
}

/// Adds the `.rs` files under `dir` to `files`.
fn collect_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_sources(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// Returns one line for each place a source outside a rule's allowed
/// prefixes holds one of its patterns. Each source is its path from the
/// workspace root, with forward slashes, and its text.
fn violations(rules: &[Rule], sources: &[(String, String)]) -> Vec<String> {
    let mut findings = Vec::new();
    for rule in rules {
        for (path, text) in sources {
            if rule.allowed.iter().any(|prefix| path.starts_with(prefix)) {
                continue;
            }
            let home = if rule.allowed.is_empty() {
                "nowhere".to_string()
            } else {
                format!("under {}", rule.allowed.join(", "))
            };
            for (index, line) in text.lines().enumerate() {
                for pattern in rule.patterns {
                    if holds(line, pattern) {
                        findings.push(format!(
                            "{}:{}: `{}` belongs {}",
                            path,
                            index + 1,
                            pattern,
                            home
                        ));
                    }
                }
            }
        }
    }
    findings
}

/// Returns whether `line` holds `pattern`. A pattern that opens with a letter
/// counts only where no letter, digit or underscore comes before it.
fn holds(line: &str, pattern: &str) -> bool {
    let opens_a_name = pattern.starts_with(|c: char| c.is_ascii_alphabetic());
    line.match_indices(pattern).any(|(at, _)| {
        !opens_a_name
            || !line[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

#[test]
fn a_pattern_that_opens_with_a_letter_matches_where_a_name_starts() {
    assert!(holds("let h = Header::parse(bytes);", "Header::parse("));
    assert!(holds("packet::Header::parse(bytes)", "Header::parse("));
    assert!(!holds("PackageHeader::parse(bytes)", "Header::parse("));
    assert!(holds(
        "PackageHeader::parse(a); Header::parse(b)",
        "Header::parse("
    ));
    assert!(holds(
        "const CAPTURE_TRIGGER_INDEX: i16 = -1;",
        "_INDEX: i16 = -"
    ));
    assert!(holds("let x = vec![0u8; 288];", "[0u8; 288]"));
}

#[test]
fn the_scanner_reports_a_pattern_outside_its_home_only() {
    let rules = [Rule {
        patterns: &["needle("],
        allowed: &["home/"],
    }];
    let sources = [
        ("home/a.rs".to_string(), "let x = needle(1);\n".to_string()),
        (
            "away/b.rs".to_string(),
            "fn f() {}\nlet y = needle(2);\n".to_string(),
        ),
        ("away/c.rs".to_string(), "let z = needle;\n".to_string()),
    ];
    let findings = violations(&rules, &sources);
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert!(findings[0].starts_with("away/b.rs:2: "), "{}", findings[0]);
}

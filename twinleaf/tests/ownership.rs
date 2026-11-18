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
const ROOTS: [&str; 7] = [
    "twinleaf/src",
    "twinleaf-data/src",
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
        allowed: &["twinleaf-data/src/logfile/reader.rs"],
    },
    // The text for a recorded packet the data state refuses.
    Rule {
        patterns: &["invalid data at byte offset"],
        allowed: &["twinleaf-data/src/logfile/reader.rs"],
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
    // A host feature, which no crate has. The host library is `twinleaf`
    // whole, and `twinleaf-data` is the part that builds without an
    // operating system.
    Rule {
        patterns: &["feature = \"host\""],
        allowed: &[],
    },
    // A segment's row rate and row period, which `SegmentView::rate_hz` and
    // `SegmentView::period` own.
    Rule {
        patterns: &[
            "f64::from(sampling_rate) / f64::from(decimation)",
            "f64::from(decimation) / f64::from(sampling_rate)",
            ".sampling_rate()) / f64::from(",
            ".decimation()) / f64::from(",
        ],
        allowed: &["twinleaf-proto/"],
    },
    // The heartbeat period, which `heartbeat::PERIOD` owns.
    Rule {
        patterns: &[
            "HEARTBEAT_INTERVAL: u64 = 2",
            "LIVENESS_TIMEOUT: Duration = Duration::",
        ],
        allowed: &[],
    },
    // The data types that decode into a column, which `decodes` owns.
    Rule {
        patterns: &[": [DataType; "],
        allowed: &["twinleaf-data/"],
    },
    // Why a writer refuses a packet, which the protocol crate's writers own
    // and report as a `WriteError`.
    Rule {
        patterns: &[
            "expect(\"payload sized above\")",
            "> WirePacket::MAX_PAYLOAD",
            "return Err(EncodeError::NoSamples)",
            "return Err(EncodeError::SampleNumberTooLarge(",
            ".ok_or(EncodeError::ValueTooLarge",
            ".is_none_or(|len| len > Packet::MAX_PAYLOAD)",
        ],
        allowed: &[],
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

/// The crates the data layer may not depend on. It reads packets through the
/// protocol crate and knows nothing of devices, transports or tasks.
const DATA_FORBIDDEN_DEPENDENCIES: [&str; 4] = ["twinleaf", "twinleaf-device", "tokio", "flume"];

#[test]
fn the_data_crate_depends_on_the_protocol_crate_alone() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest.join("../twinleaf-data/Cargo.toml");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    let named: Vec<String> = dependency_names(&text)
        .into_iter()
        .filter(|name| DATA_FORBIDDEN_DEPENDENCIES.contains(&name.as_str()))
        .collect();
    assert!(
        named.is_empty(),
        "twinleaf-data/Cargo.toml depends on {named:?}"
    );
}

/// Returns the package names that the dependency tables of a manifest name,
/// in every section whose name ends in `dependencies`, and every package a
/// dependency renames with `package`. Underscores read as hyphens.
fn dependency_names(manifest: &str) -> Vec<String> {
    let unquote = |text: &str| {
        text.trim()
            .trim_matches(|c| c == '"' || c == '\'')
            .replace('_', "-")
    };
    // A dependency table reads each key as a dependency. The table of one
    // dependency, `[dependencies.name]`, holds its fields as keys.
    let mut in_table = false;
    let mut in_single = false;
    let mut names = Vec::new();
    for line in manifest.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if let Some(header) = line.strip_prefix('[') {
            let header = header.trim_start_matches('[').trim_end_matches(']').trim();
            in_table = header.ends_with("dependencies");
            in_single = false;
            if let Some((table, name)) = header.rsplit_once('.') {
                if table.ends_with("dependencies") {
                    in_single = true;
                    names.push(unquote(name));
                }
            }
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if in_single && key == "package" {
            names.push(unquote(value));
        }
        if !in_table {
            continue;
        }
        names.push(unquote(key.split('.').next().unwrap_or(key)));
        if let Some((_, renamed)) = value.split_once("package") {
            if let Some((_, package)) = renamed.split_once('=') {
                let package = package.split([',', '}']).next().unwrap_or("");
                names.push(unquote(package));
            }
        }
    }
    names
}

#[test]
fn the_manifest_reader_finds_each_way_of_naming_a_dependency() {
    let manifest = r#"
[package]
name = "tokio"

[dependencies]
bytes = "1"
twinleaf-proto = { path = "../twinleaf-proto" }
other = { version = "1", package = "flume" }

[dependencies.twinleaf_device]
path = "../twinleaf-device"

[dependencies.renamed]
version = "1"
package = "tokio"

[target.'cfg(unix)'.dev-dependencies]
twinleaf.workspace = true

[features]
host = ["dep:memmap2"]
"#;
    assert_eq!(
        dependency_names(manifest),
        [
            "bytes",
            "twinleaf-proto",
            "other",
            "flume",
            "twinleaf-device",
            "renamed",
            "tokio",
            "twinleaf",
        ]
    );
}

//! Checks the prose of doc comments in the workspace crates.
//!
//! The test reads every `///` and `//!` line of the `twinleaf`,
//! `twinleaf-proto` and `twinleaf-tools` sources. It fails on a semicolon, a
//! colon, an em or en dash, or a spaced hyphen in prose. Code blocks,
//! headings, code spans, link targets and URLs are not prose.

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn doc_prose_has_no_semicolons_colons_or_dashes() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap_or(manifest);
    let mut files = Vec::new();
    for root in ["../twinleaf/src", "../twinleaf-proto/src", "src"] {
        if let Ok(root) = manifest.join(root).canonicalize() {
            collect_sources(&root, &mut files);
        }
    }
    assert!(
        !files.is_empty(),
        "found no sources under {}",
        manifest.display()
    );
    files.sort();

    let mut findings = Vec::new();
    for file in &files {
        let source = fs::read_to_string(file).expect("a readable source file");
        let shown = file.strip_prefix(workspace).unwrap_or(file);
        for (line, text) in check_source(&source) {
            findings.push(format!("{}:{}: {}", shown.display(), line, text));
        }
    }
    assert!(
        findings.is_empty(),
        "doc prose has a semicolon, colon or dash\n{}",
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

/// Returns the line number and text of each doc line whose prose breaks the
/// rule. Scanning stops at an inline test module.
fn check_source(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut findings = Vec::new();
    let mut in_fence = false;
    let mut code_span = None;
    for (index, line) in lines.iter().enumerate() {
        if starts_test_module(&lines[index..]) {
            break;
        }
        let Some(text) = doc_text(line) else {
            in_fence = false;
            code_span = None;
            continue;
        };
        let text = text.trim();
        if text.starts_with("```") || text.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || is_heading(text) {
            continue;
        }
        if has_forbidden(&prose(text, &mut code_span)) {
            findings.push((index + 1, text.to_string()));
        }
    }
    findings
}

/// Returns the text of a `///` or `//!` doc line, or `None` for any other
/// line.
fn doc_text(line: &str) -> Option<&str> {
    let line = line.trim_start();
    if line.starts_with("////") {
        return None;
    }
    line.strip_prefix("///")
        .or_else(|| line.strip_prefix("//!"))
}

/// Returns `true` if `lines` start with `#[cfg(test)]` on an inline module.
fn starts_test_module(lines: &[&str]) -> bool {
    let Some(rest) = lines[0].trim().strip_prefix("#[cfg(test)]") else {
        return false;
    };
    if !rest.trim().is_empty() {
        return is_inline_mod(rest);
    }
    lines[1..]
        .iter()
        .map(|line| line.trim())
        .find(|line| !line.is_empty() && !line.starts_with("#["))
        .is_some_and(is_inline_mod)
}

/// Returns `true` if `line` opens a module whose body follows inline.
fn is_inline_mod(line: &str) -> bool {
    let line = line.trim();
    let line = match line.strip_prefix("pub") {
        Some(rest) if rest.starts_with(' ') => rest.trim_start(),
        Some(rest) if rest.starts_with('(') => rest.split_once(')').map_or(rest, |(_, r)| r),
        _ => line,
    };
    line.trim_start().starts_with("mod ") && line.ends_with('{')
}

/// Returns `true` if `text` is a Markdown heading.
fn is_heading(text: &str) -> bool {
    let rest = text.trim_start_matches('#');
    rest.len() < text.len() && (rest.is_empty() || rest.starts_with(' '))
}

/// Returns the prose of one doc line. It drops a leading bullet, a link
/// reference definition, code spans, link targets and URLs. `code_span` holds
/// the length of the backtick run of a code span that continues from the
/// previous line.
fn prose(text: &str, code_span: &mut Option<usize>) -> String {
    let mut text = text;
    if code_span.is_none() {
        text = text
            .strip_prefix("- ")
            .or_else(|| text.strip_prefix("* "))
            .unwrap_or(text);
        if is_link_definition(text) {
            return String::new();
        }
    }
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let run = chars[i..].iter().take_while(|&&c| c == '`').count();
        if let Some(open) = *code_span {
            if run == open {
                *code_span = None;
            }
            i += run.max(1);
        } else if run > 0 {
            *code_span = Some(run);
            i += run;
        } else if chars[i] == ']' && chars.get(i + 1) == Some(&'(') {
            out.push(']');
            i = skip_parens(&chars, i + 1);
        } else if starts_with(&chars[i..], "https://") || starts_with(&chars[i..], "http://") {
            while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '>' {
                i += 1;
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// Returns `true` if `text` defines a link reference, as `[label]: target`.
fn is_link_definition(text: &str) -> bool {
    text.starts_with('[')
        && text
            .find("]:")
            .is_some_and(|end| !text[1..end].contains(']'))
}

/// Returns the index just past the parenthesized group that opens at
/// `open`.
fn skip_parens(chars: &[char], open: usize) -> usize {
    let mut depth = 0;
    for (i, &c) in chars.iter().enumerate().skip(open) {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
    }
    chars.len()
}

/// Returns `true` if `chars` start with `prefix`.
fn starts_with(chars: &[char], prefix: &str) -> bool {
    prefix
        .chars()
        .enumerate()
        .all(|(i, c)| chars.get(i) == Some(&c))
}

/// Returns `true` if `prose` has a semicolon, a colon, an em or en dash, or
/// a spaced hyphen.
fn has_forbidden(prose: &str) -> bool {
    let padded = format!(" {prose} ");
    prose.contains([';', ':', '\u{2014}', '\u{2013}'])
        || padded.contains(" - ")
        || padded.contains(" -- ")
}

#[test]
fn prose_drops_code_links_and_urls() {
    let mut span = None;
    assert_eq!(prose("- See [`a::b`](crate::a::b).", &mut span), "See [].");
    assert_eq!(prose("Visit https://x.io/a:b now", &mut span), "Visit  now");
    assert_eq!(prose("[`Foo`]: crate::Foo", &mut span), "");
    assert_eq!(prose("Starts `a::b", &mut span), "Starts ");
    assert_eq!(prose("c::d` ends here", &mut span), " ends here");
    assert!(span.is_none());
}

#[test]
fn rule_flags_punctuation_in_prose() {
    let source =
        "/// One; two.\n/// A: b.\n/// A - b.\n/// A-b.\n/// ```\n/// a: b;\n/// ```\n/// # A: b\n";
    let lines: Vec<usize> = check_source(source).into_iter().map(|(n, _)| n).collect();
    assert_eq!(lines, [1, 2, 3]);
}

#[test]
fn scanning_stops_at_an_inline_test_module() {
    let source = "#[cfg(test)]\nfn helper() {}\n/// A: b.\n#[cfg(test)]\nmod tests {\n/// C: d.\n";
    let lines: Vec<usize> = check_source(source).into_iter().map(|(n, _)| n).collect();
    assert_eq!(lines, [3]);
}

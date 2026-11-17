//! Diagnostics. There is one struct for every stage, and a renderer in the
//! style of rustc. The same struct is meant to become carets in the CLI, log
//! packets on the wire, and gutter markers in the editor.

use super::Span;
use std::fmt::Write;

/// Stable, greppable codes. Each has a rendered example checked in under
/// `tests/diagnostics`.
pub mod code {
    pub const UNKNOWN_CHARACTER: &str = "E0001";
    pub const SYNTAX: &str = "E0002";
    pub const STREAM_PATH: &str = "E0003";

    pub const DEFINED_TWICE: &str = "E0201";
    pub const UNKNOWN_NAME: &str = "E0202";
    pub const UNKNOWN_FUNCTION: &str = "E0203";
    pub const USED_BEFORE_DEFINITION: &str = "E0204";

    pub const SOURCE_TYPE_UNKNOWN: &str = "E0301";
    pub const ATTRIBUTE: &str = "E0302";
    pub const PARAM: &str = "E0303";

    pub const WRONG_KIND: &str = "E0401";
    pub const ARGUMENTS: &str = "E0402";
    pub const ARGUMENT_VALUE: &str = "E0403";
    pub const NO_SIGNAL: &str = "E0404";
    pub const OPERANDS: &str = "E0405";

    pub const ALIGN_RATES: &str = "E0412";
    pub const DOMAINS: &str = "E0413";
    pub const NYQUIST: &str = "E0414";

    pub const NO_CHANNEL: &str = "E0420";
    pub const UNPAIRED_CHANNELS: &str = "E0421";
    pub const UNITS: &str = "E0430";

    pub const RECORDING: &str = "E0501";
    pub const WHOLE_RUN: &str = "E0502";

    pub const UNUSED: &str = "W0001";

    /// Every code, with what it means.
    pub const ALL: &[(&str, &str)] = &[
        (UNKNOWN_CHARACTER, "a character the language does not use"),
        (SYNTAX, "the text does not parse"),
        (STREAM_PATH, "a stream path is malformed"),
        (DEFINED_TWICE, "a name is defined twice"),
        (UNKNOWN_NAME, "a name is not defined"),
        (UNKNOWN_FUNCTION, "a function does not exist"),
        (
            USED_BEFORE_DEFINITION,
            "a name is used above its definition",
        ),
        (
            SOURCE_TYPE_UNKNOWN,
            "a source's rate or channels are not known",
        ),
        (ATTRIBUTE, "an attribute is unknown or malformed"),
        (PARAM, "a param's value, unit and range disagree"),
        (
            WRONG_KIND,
            "a signal where a number belongs, or the reverse",
        ),
        (ARGUMENTS, "a call's arguments do not fit the function"),
        (ARGUMENT_VALUE, "an argument's value is not allowed"),
        (NO_SIGNAL, "a function was given no signal to work on"),
        (OPERANDS, "an operator does not apply to these operands"),
        (ALIGN_RATES, "align was given signals at different rates"),
        (DOMAINS, "signals in different domains were combined"),
        (NYQUIST, "a frequency reaches the Nyquist limit"),
        (NO_CHANNEL, "a signal has no such channel or group"),
        (UNPAIRED_CHANNELS, "channels do not pair by name"),
        (UNITS, "units disagree"),
        (
            RECORDING,
            "the spec does not fit the recording it is run over",
        ),
        (
            WHOLE_RUN,
            "a function that needs a whole run was asked for live",
        ),
        (UNUSED, "a definition is never used"),
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Note,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteKind {
    Note,
    Help,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: Option<&'static str>,
    pub message: String,
    /// The primary span, which is where the problem is.
    pub span: Span,
    /// What to say at the primary span. May be empty.
    pub label: String,
    /// Secondary spans, such as each arm's rate.
    pub labels: Vec<(Span, String)>,
    pub notes: Vec<(NoteKind, String)>,
    /// Set for diagnostics raised after the graph is running, which still
    /// point at the expression that assumed otherwise.
    pub runtime: bool,
}

impl Diagnostic {
    fn new(severity: Severity, code: &'static str, span: Span, message: String) -> Self {
        Self {
            severity,
            code: Some(code),
            message,
            span,
            label: String::new(),
            labels: Vec::new(),
            notes: Vec::new(),
            runtime: false,
        }
    }

    pub fn error(code: &'static str, span: Span, message: impl Into<String>) -> Self {
        Self::new(Severity::Error, code, span, message.into())
    }

    pub fn warning(code: &'static str, span: Span, message: impl Into<String>) -> Self {
        Self::new(Severity::Warning, code, span, message.into())
    }

    /// Something worth saying that is not wrong, and so has no code.
    pub fn remark(span: Span, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Note,
            code: None,
            ..Self::new(Severity::Note, "", span, message.into())
        }
    }

    /// Says something at the primary span.
    pub fn label(mut self, text: impl Into<String>) -> Self {
        self.label = text.into();
        self
    }

    /// Points at another span.
    pub fn secondary(mut self, span: Span, text: impl Into<String>) -> Self {
        self.labels.push((span, text.into()));
        self
    }

    pub fn note(mut self, text: impl Into<String>) -> Self {
        self.notes.push((NoteKind::Note, text.into()));
        self
    }

    pub fn help(mut self, text: impl Into<String>) -> Self {
        self.notes.push((NoteKind::Help, text.into()));
        self
    }
}

/// Where lines begin, for turning byte offsets into lines and columns.
struct Lines<'a> {
    source: &'a str,
    starts: Vec<usize>,
}

impl<'a> Lines<'a> {
    fn new(source: &'a str) -> Self {
        let mut starts = vec![0];
        starts.extend(source.match_indices('\n').map(|(i, _)| i + 1));
        Self { source, starts }
    }

    /// The zero-based line holding `offset`.
    fn line_of(&self, offset: usize) -> usize {
        self.starts.partition_point(|&start| start <= offset) - 1
    }

    fn text(&self, line: usize) -> &'a str {
        let end = self
            .starts
            .get(line + 1)
            .map_or(self.source.len(), |next| next - 1);
        self.source[self.starts[line]..end].trim_end_matches('\r')
    }

    /// How many characters precede `offset` on its line.
    fn column(&self, line: usize, offset: usize) -> usize {
        let text = self.text(line);
        let within = (offset - self.starts[line]).min(text.len());
        text[..within].chars().count()
    }
}

/// How wide a prefix of a line is once drawn, with tabs as four columns.
fn drawn_width(text: &str, chars: usize) -> usize {
    text.chars()
        .take(chars)
        .map(|c| if c == '\t' { 4 } else { 1 })
        .sum()
}

struct Mark<'a> {
    line: usize,
    column: usize,
    width: usize,
    primary: bool,
    text: &'a str,
}

/// Renders one diagnostic against the source it came from.
pub fn render(diagnostic: &Diagnostic, file: &str, source: &str) -> String {
    let lines = Lines::new(source);
    let mut out = String::new();

    let severity = match diagnostic.severity {
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Note => "note",
    };
    match diagnostic.code {
        Some(code) => writeln!(out, "{severity}[{code}]: {}", diagnostic.message),
        None => writeln!(out, "{severity}: {}", diagnostic.message),
    }
    .unwrap();

    let mark = |span: Span, text, primary| {
        let line = lines.line_of(span.start);
        let column = lines.column(line, span.start);
        // A span that runs past its first line is underlined to that line's end.
        let end = lines.column(line, span.end.max(span.start));
        Mark {
            line,
            column,
            width: end.saturating_sub(column).max(1),
            primary,
            text,
        }
    };
    let mut marks = vec![mark(diagnostic.span, diagnostic.label.as_str(), true)];
    marks.extend(
        diagnostic
            .labels
            .iter()
            .map(|(span, text)| mark(*span, text.as_str(), false)),
    );
    let location = (marks[0].line + 1, marks[0].column + 1);
    marks.sort_by_key(|m| (m.line, m.column, !m.primary));

    let gutter = marks
        .iter()
        .map(|m| (m.line + 1).to_string().len())
        .max()
        .unwrap_or(1);
    let pad = " ".repeat(gutter);
    writeln!(out, "{pad}--> {file}:{}:{}", location.0, location.1).unwrap();
    writeln!(out, "{pad} |").unwrap();

    let mut drawn: Option<usize> = None;
    for mark in &marks {
        if drawn != Some(mark.line) {
            if drawn.is_some_and(|previous| mark.line > previous + 1) {
                writeln!(out, "{pad}...").unwrap();
            }
            let text = lines.text(mark.line).replace('\t', "    ");
            let row = format!("{:>gutter$} | {text}", mark.line + 1);
            writeln!(out, "{}", row.trim_end()).unwrap();
            drawn = Some(mark.line);
        }
        let text = lines.text(mark.line);
        let indent = " ".repeat(drawn_width(text, mark.column));
        let stroke = if mark.primary { "^" } else { "-" };
        let underline = stroke.repeat(mark.width);
        let row = format!("{pad} | {indent}{underline} {}", mark.text);
        writeln!(out, "{}", row.trim_end()).unwrap();
    }

    if !diagnostic.notes.is_empty() {
        writeln!(out, "{pad} |").unwrap();
    }
    for (kind, text) in &diagnostic.notes {
        let kind = match kind {
            NoteKind::Note => "note",
            NoteKind::Help => "help",
        };
        for (index, line) in text.lines().enumerate() {
            let row = if index == 0 {
                format!("{pad} = {kind}: {line}")
            } else {
                format!("{pad}         {line}")
            };
            writeln!(out, "{}", row.trim_end()).unwrap();
        }
    }
    out
}

/// Renders diagnostics one after another, a blank line between them.
pub fn render_all(diagnostics: &[Diagnostic], file: &str, source: &str) -> String {
    diagnostics
        .iter()
        .map(|d| render(d, file, source))
        .collect::<Vec<_>>()
        .join("\n")
}

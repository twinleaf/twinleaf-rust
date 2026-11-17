//! The language. Text goes in, and a checked program and diagnostics come out.
//!
//! ```text
//! source text ─► tokens ─► lossless syntax tree ─► AST ─► checked program
//!                lexer     parser                  ast    check
//! ```
//!
//! Every stage reports through the same [`Diagnostic`], and every stage keeps
//! byte spans into the source, so that an editor can change a spec by
//! splicing text rather than by regenerating it.

pub mod ast;
pub mod builtins;
pub mod check;
pub mod diag;
pub mod dot;
pub mod lexer;
pub mod outline;
pub mod parser;
pub mod syntax;
pub mod typst;
pub mod units;

pub use crate::library::Library;
pub use check::Program;
pub use diag::{Diagnostic, Severity};

/// A byte range in the source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// The empty span at `offset`.
    pub fn at(offset: usize) -> Self {
        Self::new(offset, offset)
    }

    /// The smallest span covering both.
    pub fn to(self, other: Self) -> Self {
        Self::new(self.start.min(other.start), self.end.max(other.end))
    }

    /// The text this span covers.
    pub fn text(self, source: &str) -> &str {
        &source[self.start..self.end]
    }
}

/// Everything known about a spec once it has been read.
pub struct Analysis {
    /// The lossless syntax tree, with every byte of the source in order.
    pub syntax: syntax::SyntaxNode,
    /// The checked program. When there are errors it is incomplete.
    pub program: Program,
    /// Every diagnostic from every stage, in source order.
    pub diagnostics: Vec<Diagnostic>,
}

impl Analysis {
    /// Returns whether any diagnostic is an error.
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == Severity::Error)
    }
}

/// Parses and checks a spec that calls the public functions.
pub fn analyze(source: &str) -> Analysis {
    analyze_with(source, &Library::standard())
}

/// Parses and checks a spec that may call any function in `library`.
pub fn analyze_with(source: &str, library: &Library) -> Analysis {
    let parse = parser::parse(source);
    let file = ast::lower(&parse.root, source);
    let (program, checked) = check::check(&file, source, library);
    let mut diagnostics = parse.diagnostics;
    diagnostics.extend(checked);
    diagnostics.sort_by_key(|d| d.span.start);
    Analysis {
        syntax: parse.root,
        program,
        diagnostics,
    }
}

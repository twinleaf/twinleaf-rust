//! What an editor needs to know about a spec's text. That is how to colour
//! it, what is wrong with it, the graph it describes, and where in the text
//! each part of that graph is written.
//!
//! The editor never writes a spec out from the graph. Every gesture on the
//! canvas is a splice of the text between offsets this module hands out.
//! Dragging a param replaces its digits, removing a stage deletes its text,
//! and adding one inserts ` | stage(…)`. The text is then read again, and
//! the graph is whatever it says.
//!
//! Offsets here count UTF-16 code units, as a text view does, and not the
//! bytes the rest of the language counts.

use super::builtins::{ArgKind, Builtin, Signals, BUILTINS};
use super::check::{Op, Program};
use super::diag::{self, Diagnostic, NoteKind, Severity};
use super::dot::{arguments, scalar};
use super::lexer::split_number;
use super::syntax::{SyntaxElement, SyntaxKind, SyntaxNode};
use super::{analyze_with, ast, Span};
use crate::library::Library;
use crate::lower::{RUNNABLE, WHOLE_RUN};

/// A stretch of the text, in UTF-16 code units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Range {
    pub start: usize,
    pub end: usize,
}

/// A spec as an editor sees it.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Outline {
    /// Whether the text checks. A text that does not still has an outline,
    /// of as much as could be read.
    pub ok: bool,
    /// How to colour the text. In order, and none overlap.
    pub marks: Vec<Mark>,
    pub problems: Vec<Problem>,
    /// The graph, each node after the nodes that feed it.
    pub nodes: Vec<Shape>,
    pub params: Vec<Knob>,
}

/// A stretch of text and what kind of thing is written there.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Mark {
    pub range: Range,
    /// One of `keyword`, `definition`, `name`, `param`, `function`,
    /// `argument`, `channel`, `number`, `unit`, `path`, `attribute`,
    /// `operator`, `punctuation`, `comment` and `invalid`.
    pub class: &'static str,
}

/// Something wrong with the text.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Problem {
    /// `error`, `warning` or `note`.
    pub severity: &'static str,
    pub code: Option<&'static str>,
    pub message: String,
    /// What to say at the place itself. May be empty.
    pub label: String,
    pub range: Range,
    /// Counted from 1.
    pub line: usize,
    pub column: usize,
    /// Other places that bear on it, such as each arm's rate.
    pub related: Vec<Related>,
    pub notes: Vec<Note>,
    /// The whole of it as a terminal shows it.
    pub rendered: String,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Related {
    pub range: Range,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Note {
    /// `note` or `help`.
    pub kind: &'static str,
    pub text: String,
}

/// One node of the graph, which is a source, an operation, or an output.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Shape {
    pub id: String,
    /// `source`, `function`, `operator`, `select` or `output`.
    pub kind: &'static str,
    /// What it is, such as `notch`, `/0/field`, `−`, `.a` or `/255/gradient`.
    pub title: String,
    /// What it is given, such as `line, q = 30`. May be empty.
    pub detail: String,
    /// The name the spec gives it, if it has one.
    pub name: Option<String>,
    /// The nodes that feed it, in order.
    pub inputs: Vec<String>,
    /// Its signal in words, such as `500 Hz · 6 ch · nT`.
    pub signal: String,
    pub rate: f64,
    pub channels: Vec<String>,
    /// Whether there is signal processing behind it yet.
    pub runs: bool,
    /// All of the text that makes it, inputs included.
    pub range: Range,
    /// The text that is this node and no other, such as the call at the end
    /// of a pipeline, or the sign between two operands.
    pub focus: Range,
    /// The text to delete to take a stage out of its pipeline. Only a
    /// piped stage has one.
    pub remove: Option<Range>,
    /// Where a stage may be piped on after this node.
    pub append: Option<usize>,
    /// The stages that may be piped on there, each as it would be written
    /// for a signal at this rate.
    pub stages: Vec<Stage>,
}

/// A stage to pipe on, written out.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Stage {
    pub function: &'static str,
    /// What to insert, the pipe included, such as ` | lowpass(50 Hz)`.
    pub text: String,
}

/// A param, and where its value is written.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Knob {
    pub name: String,
    /// Empty for a param without one.
    pub unit: String,
    pub value: f64,
    pub low: Option<f64>,
    pub high: Option<f64>,
    /// The whole definition.
    pub range: Range,
    /// The digits of the value, and its sign. This is what to replace to
    /// set it.
    pub digits: Range,
}

/// Reads `source` and says what an editor needs to know about it.
pub fn outline(source: &str) -> Outline {
    // The text may be bound for a graph with functions of its own. One
    // that is not known here is drawn as passing its input on and remarked
    // on, and the graph that runs the text has the last word.
    let analysis = analyze_with(source, &Library::standard().trusting());
    let offsets = Offsets::new(source);
    let params: Vec<&str> = analysis
        .program
        .params
        .iter()
        .map(|param| param.name.as_str())
        .collect();
    let mut marks = Vec::new();
    mark(&analysis.syntax, source, &params, &mut marks);
    Outline {
        ok: !analysis.has_errors(),
        marks: joined(marks)
            .into_iter()
            .map(|(span, class)| Mark {
                range: offsets.range(span),
                class,
            })
            .collect(),
        problems: analysis
            .diagnostics
            .iter()
            .map(|diagnostic| problem(diagnostic, source, &offsets))
            .collect(),
        nodes: shapes(&analysis.program, &analysis.syntax, source, &offsets),
        params: analysis
            .program
            .params
            .iter()
            .map(|param| Knob {
                name: param.name.clone(),
                // A param without a unit has nothing to show for one.
                unit: match param.unit.is_none() {
                    true => String::new(),
                    false => param.unit.to_string(),
                },
                value: param.value,
                low: param.range.map(|(low, _)| low),
                high: param.range.map(|(_, high)| high),
                range: offsets.range(param.span),
                digits: offsets.range(param.value_span),
            })
            .collect(),
    }
}

/// Byte offsets to UTF-16 offsets.
struct Offsets(Vec<usize>);

impl Offsets {
    fn new(source: &str) -> Self {
        let mut units = Vec::with_capacity(source.len() + 1);
        let mut count = 0;
        for character in source.chars() {
            units.extend(std::iter::repeat_n(count, character.len_utf8()));
            count += character.len_utf16();
        }
        units.push(count);
        Self(units)
    }

    fn at(&self, offset: usize) -> usize {
        self.0[offset.min(self.0.len() - 1)]
    }

    fn range(&self, span: Span) -> Range {
        Range {
            start: self.at(span.start),
            end: self.at(span.end.max(span.start)),
        }
    }
}

/// Runs together the marks that touch and are of one class, so that a path
/// or `@rate` is one stretch.
fn joined(marks: Vec<(Span, &'static str)>) -> Vec<(Span, &'static str)> {
    let mut out: Vec<(Span, &'static str)> = Vec::with_capacity(marks.len());
    for (span, class) in marks {
        if span.start == span.end {
            continue;
        }
        match out.last_mut() {
            Some((last, same)) if *same == class && last.end == span.start => last.end = span.end,
            _ => out.push((span, class)),
        }
    }
    out
}

/// Classes every token under `node` that is worth a colour.
fn mark(node: &SyntaxNode, source: &str, params: &[&str], out: &mut Vec<(Span, &'static str)>) {
    use SyntaxKind::*;
    // Everything in a path or a unit is the path or the unit.
    let whole = match node.kind {
        Path => Some("path"),
        Unit => Some("unit"),
        _ => None,
    };
    for child in &node.children {
        let token = match child {
            SyntaxElement::Node(child) => {
                mark(child, source, params, out);
                continue;
            }
            SyntaxElement::Token(token) => token,
        };
        let class = match token.kind {
            Whitespace | Eof => continue,
            Comment => "comment",
            Unknown => "invalid",
            _ if whole.is_some() => whole.unwrap_or_default(),
            SourceKw | ParamKw | LetKw | OutKw | FnKw | InKw => "keyword",
            Number => {
                // `10s` is a number and then a unit.
                let text = token.span.text(source);
                let suffix = split_number(text).1.map_or(0, str::len);
                let split = token.span.end - suffix;
                out.push((Span::new(token.span.start, split), "number"));
                out.push((Span::new(split, token.span.end), "unit"));
                continue;
            }
            Ident => match node.kind {
                SourceItem | ParamItem | LetItem | OutItem | FnItem => "definition",
                Attr => "attribute",
                Call => "function",
                Arg => "argument",
                Select => "channel",
                // `10 s`: the unit of a quantity.
                Literal => "unit",
                NameRef if params.contains(&token.span.text(source)) => "param",
                _ => "name",
            },
            At => "attribute",
            Dot if node.kind == Select => "channel",
            Eq | Pipe | DotDot | Plus | Minus | Star | Slash | Caret => "operator",
            Dot | Comma | Colon | LParen | RParen | LBracket | RBracket => "punctuation",
            // The rest are kinds of node, which no token has.
            _ => continue,
        };
        out.push((token.span, class));
    }
}

fn problem(diagnostic: &Diagnostic, source: &str, offsets: &Offsets) -> Problem {
    let before = &source[..diagnostic.span.start.min(source.len())];
    let line_start = before.rfind('\n').map_or(0, |at| at + 1);
    Problem {
        severity: match diagnostic.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Note => "note",
        },
        code: diagnostic.code,
        message: diagnostic.message.clone(),
        label: diagnostic.label.clone(),
        range: offsets.range(diagnostic.span),
        line: before.matches('\n').count() + 1,
        column: before[line_start..].chars().count() + 1,
        related: diagnostic
            .labels
            .iter()
            .map(|(span, label)| Related {
                range: offsets.range(*span),
                label: label.clone(),
            })
            .collect(),
        notes: diagnostic
            .notes
            .iter()
            .map(|(kind, text)| Note {
                kind: match kind {
                    NoteKind::Note => "note",
                    NoteKind::Help => "help",
                },
                text: text.clone(),
            })
            .collect(),
        rendered: diag::render(diagnostic, "dsp.spec", source),
    }
}

/// The syntax nodes from the root down to the innermost one that is
/// exactly `span`, or as far as the tree goes towards it.
fn path_to(root: &SyntaxNode, span: Span) -> Vec<&SyntaxNode> {
    let mut path = vec![root];
    let mut at = root;
    while let Some(child) = at
        .nodes()
        .find(|child| child.span.start <= span.start && span.end <= child.span.end)
    {
        path.push(child);
        at = child;
    }
    path
}

/// Where in the text a node of the program is written.
struct Written {
    focus: Span,
    remove: Option<Span>,
    append: Option<usize>,
}

fn written(root: &SyntaxNode, span: Span) -> Written {
    use SyntaxKind::*;
    let path = path_to(root, span);
    let exact: Vec<usize> = (0..path.len())
        .filter(|&index| path[index].span == span)
        .collect();
    let (Some(&outer), Some(&inner)) = (exact.first(), exact.last()) else {
        return Written {
            focus: span,
            remove: None,
            append: None,
        };
    };
    let node = path[inner];
    let operator = |kinds: &[SyntaxKind]| {
        node.tokens()
            .find(|token| kinds.contains(&token.kind))
            .map(|token| token.span)
    };
    let focus = match node.kind {
        Pipeline => node.node(Call).map(|call| call.span),
        Binary => operator(&[Plus, Minus, Star, Slash]),
        Conversion => operator(&[InKw]).map(|keyword| Span::new(keyword.start, span.end)),
        _ => None,
    }
    .unwrap_or(span);
    // Taking a stage out leaves what was piped into it.
    let remove = match node.kind {
        Pipeline => node
            .nodes()
            .next()
            .map(|input| Span::new(input.span.end, span.end)),
        _ => None,
    };
    // A stage can follow only where a pipe would end the expression: a
    // definition's whole value, what is already piped onwards, or
    // anything in parentheses or passed as an argument. After an operand
    // it would need parentheses of its own, and a conversion comes last.
    // An argument written without a name is a node around its expression
    // and no wider than it.
    let parent = outer.checked_sub(1).map(|index| path[index]);
    let first = |parent: &SyntaxNode| parent.nodes().next().is_some_and(|n| n.span == span);
    let fits = path[outer].kind == Arg
        || parent.is_some_and(|parent| match parent.kind {
            LetItem | OutItem | FnItem | Paren | Arg => true,
            Pipeline | Conversion => first(parent),
            _ => false,
        });
    Written {
        focus,
        remove,
        append: (fits && node.kind != Conversion).then_some(span.end),
    }
}

fn shapes(program: &Program, root: &SyntaxNode, source: &str, offsets: &Offsets) -> Vec<Shape> {
    // The names `let` gives to nodes: the node written as the whole of
    // each value, parentheses aside.
    let mut names: Vec<Option<String>> = vec![None; program.nodes.len()];
    let file = ast::lower(root, source);
    for item in &file.items {
        let ast::Item::Let(binding) = item else {
            continue;
        };
        let mut value = &binding.value;
        while let ast::Expr::Paren { inner, .. } = value {
            value = inner;
        }
        let span = value.span();
        let made = program.nodes.iter().rposition(|node| node.span == span);
        if let (Some(node), Some(name)) = (made, &binding.name) {
            names[node] = Some(name.text.clone());
        }
    }

    let signal = |node: usize| program.signal_of(node);
    let channels = |node: usize| -> Vec<String> {
        let channels = &program.nodes[node].ty.channels;
        channels.iter().map(|c| c.name.clone()).collect()
    };

    let mut shapes = Vec::with_capacity(program.nodes.len() + program.outputs.len());
    for (index, node) in program.nodes.iter().enumerate() {
        let rate = program.rate_of(index);
        let (kind, title, detail, runs) = match &node.op {
            Op::Source { source } => (
                "source",
                program.sources[*source].path.to_string(),
                String::new(),
                true,
            ),
            Op::Select { name } => ("select", format!(".{name}"), String::new(), true),
            Op::Call { function, args, .. } => (
                "function",
                (*function).to_owned(),
                arguments(args).join(", "),
                // One taken on trust is taken to run.
                program.library.find(function).is_none() || program.library.runs(function),
            ),
            Op::Add => ("operator", "+".to_owned(), String::new(), true),
            Op::Subtract => ("operator", "−".to_owned(), String::new(), true),
            Op::Scale { factor, divide } => (
                "operator",
                if *divide { "÷" } else { "×" }.to_owned(),
                scalar(program, &factor.expr),
                true,
            ),
            Op::Negate => ("operator", "−".to_owned(), String::new(), true),
            Op::Declare { unit } => ("operator", format!("in {unit}"), String::new(), true),
        };
        let place = match &node.op {
            // A source is its whole definition, and nothing is piped onto
            // a definition: it is used by name.
            Op::Source { .. } => Written {
                focus: node.span,
                remove: None,
                append: None,
            },
            _ => written(root, node.span),
        };
        let name = match &node.op {
            Op::Source { source } => Some(program.sources[*source].name.clone()),
            _ => names[index].take(),
        };
        shapes.push(Shape {
            id: format!("n{index}"),
            kind,
            title,
            detail,
            name,
            inputs: node
                .inputs
                .iter()
                .map(|input| format!("n{input}"))
                .collect(),
            signal: signal(index),
            rate,
            channels: channels(index),
            runs,
            range: offsets.range(node.span),
            focus: offsets.range(place.focus),
            remove: place.remove.map(|span| offsets.range(span)),
            append: place.append.map(|offset| offsets.at(offset)),
            stages: match place.append {
                Some(_) => stages(rate),
                None => Vec::new(),
            },
        });
    }
    for (index, output) in program.outputs.iter().enumerate() {
        // A stage piped onto an output goes at the end of its value, even
        // when that is no more than a name. A conversion comes last, so
        // nothing follows one.
        let value = file.items.iter().find_map(|item| match item {
            ast::Item::Out(out) if out.span == output.span => Some(&out.value),
            _ => None,
        });
        let append = value
            .filter(|value| !matches!(value, ast::Expr::In { .. } | ast::Expr::Missing(_)))
            .map(|value| value.span().end);
        let rate = program.rate_of(output.node);
        shapes.push(Shape {
            id: format!("out{index}"),
            kind: "output",
            title: format!("/255/{}", output.name),
            detail: String::new(),
            name: Some(output.name.clone()),
            inputs: vec![format!("n{}", output.node)],
            signal: signal(output.node),
            rate,
            channels: channels(output.node),
            runs: true,
            range: offsets.range(output.span),
            focus: offsets.range(output.span),
            remove: None,
            append: append.map(|offset| offsets.at(offset)),
            stages: match append {
                Some(_) => stages(rate),
                None => Vec::new(),
            },
        });
    }
    shapes
}

/// The stages that can be piped onto a signal at `rate`, each written with
/// arguments that suit it.
fn stages(rate: f64) -> Vec<Stage> {
    BUILTINS
        .iter()
        .filter(|builtin| {
            builtin.signals == Signals::One
                && builtin.named_signal.is_none()
                && RUNNABLE.contains(&builtin.name)
                // An editor edits a graph that is running, which cannot
                // run what needs a whole recording.
                && !WHOLE_RUN.contains(&builtin.name)
        })
        .map(|builtin| Stage {
            function: builtin.name,
            text: format!(" | {}", written_for(builtin, rate)),
        })
        .collect()
}

/// A call of `builtin` with every argument it requires, for a signal at
/// `rate`.
fn written_for(builtin: &Builtin, rate: f64) -> String {
    let args: Vec<String> = builtin
        .args
        .iter()
        .filter(|arg| arg.required)
        .map(|arg| match arg.kind {
            ArgKind::Frequency => {
                let hertz = match (builtin.name, arg.name) {
                    // Mains, where the signal reaches that far.
                    ("notch", _) if rate > 150.0 => 60.0,
                    (_, "lo") | ("highpass", _) => round_down(rate / 100.0),
                    _ => round_down(rate / 10.0),
                };
                format!("{hertz} Hz")
            }
            ArgKind::Rate => format!("{} Hz", round_down(rate / 2.0)),
            ArgKind::Time | ArgKind::Span => "1 s".to_owned(),
            ArgKind::Number => "1".to_owned(),
            ArgKind::Integer { min } => min.to_string(),
            ArgKind::Choice(words) => words.first().copied().unwrap_or_default().to_owned(),
        })
        .collect();
    format!("{}({})", builtin.name, args.join(", "))
}

/// The largest of 1, 2 and 5 times a power of ten that is no more than
/// `value`.
fn round_down(value: f64) -> f64 {
    let exponent = value.log10().floor();
    let decade = 10f64.powf(exponent);
    let leading = value / decade;
    let step = if leading >= 5.0 {
        5.0
    } else if leading >= 2.0 {
        2.0
    } else {
        1.0
    };
    // Written through its decimal form, so that 0.2 reads as 0.2.
    format!("{step}e{exponent}").parse().unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_down_to_one_two_or_five() {
        assert_eq!(round_down(100.0), 100.0);
        assert_eq!(round_down(48.0), 20.0);
        assert_eq!(round_down(5.0), 5.0);
        assert_eq!(round_down(0.25), 0.2);
        assert_eq!(round_down(999.0), 500.0);
    }

    #[test]
    fn offsets_count_utf16_code_units() {
        // `µ` is two bytes and one code unit; an emoji is four and two.
        let offsets = Offsets::new("aµ😀b");
        assert_eq!(offsets.at(0), 0);
        assert_eq!(offsets.at(1), 1);
        assert_eq!(offsets.at(3), 2);
        assert_eq!(offsets.at(7), 4);
        assert_eq!(offsets.at(8), 5);
    }
}

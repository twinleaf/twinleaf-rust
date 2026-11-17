//! The abstract syntax tree, which is the syntax tree with the punctuation
//! resolved and the spans kept.
//!
//! Lowering never fails. A part the parser could not read is `None` or
//! [`Expr::Missing`]. The parser has already reported it, and the checker
//! treats whatever depends on it as unknown rather than reporting it again.

use super::lexer::split_number;
use super::syntax::{SyntaxKind, SyntaxNode};
use super::Span;

#[derive(Debug, Clone, PartialEq)]
pub struct Ident {
    pub text: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct File {
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Source(Source),
    Param(Param),
    Let(Let),
    Out(Out),
    Fn(Function),
}

impl Item {
    pub fn name(&self) -> Option<&Ident> {
        match self {
            Self::Source(item) => item.name.as_ref(),
            Self::Param(item) => item.name.as_ref(),
            Self::Let(item) => item.name.as_ref(),
            Self::Out(item) => item.name.as_ref(),
            Self::Fn(item) => item.name.as_ref(),
        }
    }

    pub fn span(&self) -> Span {
        match self {
            Self::Source(item) => item.span,
            Self::Param(item) => item.span,
            Self::Let(item) => item.span,
            Self::Out(item) => item.span,
            Self::Fn(item) => item.span,
        }
    }
}

/// `source a = /0/field @rate(1000)`
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub span: Span,
    pub name: Option<Ident>,
    pub path: Option<Path>,
    pub attrs: Vec<Attr>,
}

/// A stream path as written, such as `/0/field/x`.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub span: Span,
    pub text: String,
}

/// `@rate(1000)`
#[derive(Debug, Clone, PartialEq)]
pub struct Attr {
    pub span: Span,
    pub name: Option<Ident>,
    pub args: Vec<Arg>,
}

/// `param line : Hz = 60 [45 .. 65]`
#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub span: Span,
    pub name: Option<Ident>,
    pub unit: Option<Ident>,
    pub value: Option<Number>,
    pub range: Option<Range>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub span: Span,
    pub low: Option<Number>,
    pub high: Option<Number>,
}

/// A number as written, sign applied, with its unit suffix if it has one.
#[derive(Debug, Clone, PartialEq)]
pub struct Number {
    pub span: Span,
    /// The sign and digits alone, without the unit. This is what to replace
    /// to change the value and nothing else.
    pub digits: Span,
    pub value: f64,
    pub unit: Option<String>,
}

/// `let clean = fr | notch(line)`
#[derive(Debug, Clone, PartialEq)]
pub struct Let {
    pub span: Span,
    pub name: Option<Ident>,
    /// As far as it was read, when the definition is broken.
    pub value: Expr,
    /// The parser gave up partway through this definition.
    pub broken: bool,
}

/// `out gradient : nT = clean.a - clean.b`
#[derive(Debug, Clone, PartialEq)]
pub struct Out {
    pub span: Span,
    pub name: Option<Ident>,
    pub unit: Option<Ident>,
    /// As far as it was read, when the definition is broken.
    pub value: Expr,
    /// The parser gave up partway through this definition.
    pub broken: bool,
}

/// A function, as in `fn clean(s) = s | notch(60)`. It is an expression
/// with names left open, to be written once and used in several places. A
/// call of it is the expression with the names filled in, and nothing more.
#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub span: Span,
    pub name: Option<Ident>,
    /// What it is given, in order.
    pub params: Vec<Ident>,
    /// As far as it was read, when the definition is broken.
    pub body: Expr,
    /// The parser gave up partway through this definition.
    pub broken: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl BinaryOp {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Name(Ident),
    Number(Number),
    Path(Path),
    Call(Call),
    /// `input | call(…)`, which is `call(input, …)`.
    Pipe {
        span: Span,
        input: Box<Expr>,
        call: Call,
    },
    Binary {
        span: Span,
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Negate {
        span: Span,
        operand: Box<Expr>,
    },
    /// `base.name`
    Select {
        span: Span,
        base: Box<Expr>,
        name: Ident,
    },
    Paren {
        span: Span,
        inner: Box<Expr>,
    },
    /// `value in unit`
    In {
        span: Span,
        value: Box<Expr>,
        /// The unit as written, without its spaces.
        unit: Ident,
    },
    /// The parser could not read an expression here.
    Missing(Span),
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Self::Name(ident) => ident.span,
            Self::Number(number) => number.span,
            Self::Path(path) => path.span,
            Self::Call(call) => call.span,
            Self::Pipe { span, .. }
            | Self::Binary { span, .. }
            | Self::Negate { span, .. }
            | Self::Select { span, .. }
            | Self::Paren { span, .. }
            | Self::In { span, .. }
            | Self::Missing(span) => *span,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub span: Span,
    pub name: Ident,
    pub args: Vec<Arg>,
}

/// One argument, named or positional.
#[derive(Debug, Clone, PartialEq)]
pub struct Arg {
    pub span: Span,
    pub name: Option<Ident>,
    pub value: Expr,
}

/// Lowers a syntax tree into an AST.
pub fn lower(root: &SyntaxNode, source: &str) -> File {
    let lower = Lower { source };
    File {
        items: root.nodes().filter_map(|node| lower.item(node)).collect(),
    }
}

struct Lower<'s> {
    source: &'s str,
}

impl Lower<'_> {
    fn ident(&self, node: &SyntaxNode) -> Option<Ident> {
        node.token(SyntaxKind::Ident).map(|token| Ident {
            text: token.span.text(self.source).to_owned(),
            span: token.span,
        })
    }

    fn item(&self, node: &SyntaxNode) -> Option<Item> {
        let span = node.span;
        let name = self.ident(node);
        let unit = || node.node(SyntaxKind::Unit).map(|unit| self.unit(unit));
        // The parser gave up partway through this definition. Its name still
        // counts, so that uses of it are not reported as unknown, but what
        // was read of its value is not to be trusted.
        let broken = node.node(SyntaxKind::Error).is_some();
        Some(match node.kind {
            SyntaxKind::SourceItem => Item::Source(Source {
                span,
                name,
                path: node
                    .node(SyntaxKind::Path)
                    .filter(|_| !broken)
                    .map(|path| self.path(path)),
                attrs: node
                    .nodes()
                    .filter(|child| child.kind == SyntaxKind::Attr)
                    .map(|attr| Attr {
                        span: attr.span,
                        name: self.ident(attr),
                        args: self.args(attr),
                    })
                    .collect(),
            }),
            SyntaxKind::ParamItem => Item::Param(Param {
                span,
                name,
                unit: unit(),
                value: node
                    .node(SyntaxKind::Literal)
                    .filter(|_| !broken)
                    .and_then(|literal| self.number(literal)),
                range: node.node(SyntaxKind::Range).map(|range| {
                    let mut ends = range
                        .nodes()
                        .filter(|child| child.kind == SyntaxKind::Literal)
                        .map(|literal| self.number(literal));
                    Range {
                        span: range.span,
                        low: ends.next().flatten(),
                        high: ends.next().flatten(),
                    }
                }),
            }),
            SyntaxKind::LetItem => Item::Let(Let {
                span,
                name,
                value: self.child_expr(node),
                broken,
            }),
            SyntaxKind::OutItem => Item::Out(Out {
                span,
                name,
                unit: unit(),
                value: self.child_expr(node),
                broken,
            }),
            SyntaxKind::FnItem => Item::Fn(Function {
                span,
                name,
                // The names written directly in the definition: its own,
                // and then what it is given.
                params: node
                    .tokens()
                    .filter(|token| token.kind == SyntaxKind::Ident)
                    .skip(1)
                    .map(|token| Ident {
                        text: token.span.text(self.source).to_owned(),
                        span: token.span,
                    })
                    .collect(),
                body: self.child_expr(node),
                broken,
            }),
            _ => return None,
        })
    }

    fn path(&self, node: &SyntaxNode) -> Path {
        Path {
            span: node.span,
            text: node.span.text(self.source).to_owned(),
        }
    }

    fn number(&self, node: &SyntaxNode) -> Option<Number> {
        let token = node.token(SyntaxKind::Number)?;
        let (value, suffix) = split_number(token.span.text(self.source));
        let negative = node.token(SyntaxKind::Minus).is_some();
        // The unit is attached, as in `10s`, or follows, as in `10 s`.
        let unit = suffix
            .map(str::to_owned)
            .or_else(|| self.ident(node).map(|unit| unit.text));
        let unit_len = suffix.map_or(0, str::len);
        Some(Number {
            span: node.span,
            digits: Span::new(node.span.start, token.span.end - unit_len),
            value: if negative { -value } else { value },
            unit,
        })
    }

    /// A unit as written, without its spaces.
    fn unit(&self, node: &SyntaxNode) -> Ident {
        Ident {
            text: node
                .span
                .text(self.source)
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect(),
            span: node.span,
        }
    }

    fn args(&self, node: &SyntaxNode) -> Vec<Arg> {
        node.nodes()
            .filter(|child| child.kind == SyntaxKind::Arg)
            .map(|arg| Arg {
                span: arg.span,
                // Only a named argument holds a name token of its own. A
                // positional name sits inside a `NameRef` node.
                name: self.ident(arg),
                value: self.child_expr(arg),
            })
            .collect()
    }

    /// The `n`th expression directly under `node`.
    fn nth_expr(&self, node: &SyntaxNode, n: usize) -> Expr {
        node.nodes()
            .filter(|child| child.kind.is_expr())
            .nth(n)
            .map_or(Expr::Missing(Span::at(node.span.end)), |child| {
                self.expr(child)
            })
    }

    fn child_expr(&self, node: &SyntaxNode) -> Expr {
        self.nth_expr(node, 0)
    }

    fn call(&self, node: &SyntaxNode) -> Option<Call> {
        Some(Call {
            span: node.span,
            name: self.ident(node)?,
            args: self.args(node),
        })
    }

    fn expr(&self, node: &SyntaxNode) -> Expr {
        let span = node.span;
        let missing = Expr::Missing(span);
        match node.kind {
            SyntaxKind::NameRef => self.ident(node).map_or(missing, Expr::Name),
            SyntaxKind::Literal => self.number(node).map_or(missing, Expr::Number),
            SyntaxKind::Path => Expr::Path(self.path(node)),
            SyntaxKind::Call => self.call(node).map_or(missing, Expr::Call),
            SyntaxKind::Pipeline => {
                // The input comes first and the stage, always a call, second.
                let mut parts = node.nodes().filter(|child| child.kind.is_expr());
                let input = parts.next().map(|child| self.expr(child));
                let stage = parts
                    .next()
                    .filter(|child| child.kind == SyntaxKind::Call)
                    .and_then(|child| self.call(child));
                match (input, stage) {
                    (Some(input), Some(call)) => Expr::Pipe {
                        span,
                        input: Box::new(input),
                        call,
                    },
                    _ => missing,
                }
            }
            SyntaxKind::Binary => {
                let op = node.tokens().find_map(|token| match token.kind {
                    SyntaxKind::Plus => Some(BinaryOp::Add),
                    SyntaxKind::Minus => Some(BinaryOp::Sub),
                    SyntaxKind::Star => Some(BinaryOp::Mul),
                    SyntaxKind::Slash => Some(BinaryOp::Div),
                    _ => None,
                });
                match op {
                    Some(op) => Expr::Binary {
                        span,
                        op,
                        left: Box::new(self.nth_expr(node, 0)),
                        right: Box::new(self.nth_expr(node, 1)),
                    },
                    None => missing,
                }
            }
            SyntaxKind::Negation => Expr::Negate {
                span,
                operand: Box::new(self.child_expr(node)),
            },
            SyntaxKind::Select => match self.ident(node) {
                Some(name) => Expr::Select {
                    span,
                    base: Box::new(self.child_expr(node)),
                    name,
                },
                None => missing,
            },
            SyntaxKind::Paren => Expr::Paren {
                span,
                inner: Box::new(self.child_expr(node)),
            },
            SyntaxKind::Conversion => match node.node(SyntaxKind::Unit) {
                Some(unit) => Expr::In {
                    span,
                    value: Box::new(self.child_expr(node)),
                    unit: self.unit(unit),
                },
                None => missing,
            },
            _ => missing,
        }
    }
}

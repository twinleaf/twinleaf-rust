//! The lossless syntax tree.
//!
//! Every byte of the source belongs to exactly one token, comments and
//! whitespace included, and the tree holds every token in order. Nothing is
//! normalized away, so an edit is a splice of the source between two spans
//! the tree hands out, and the rest of the file is untouched.

use super::Span;

/// What a token or a tree node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyntaxKind {
    // Tokens the parser skips.
    Whitespace,
    Comment,
    /// Characters the language does not use.
    Unknown,

    // Tokens.
    Ident,
    /// A number with an optional unit suffix, such as `60`, `0.1` or `10s`.
    Number,
    SourceKw,
    ParamKw,
    LetKw,
    OutKw,
    FnKw,
    /// `in`, which converts to a unit.
    InKw,
    Eq,
    Pipe,
    Dot,
    DotDot,
    Comma,
    Colon,
    LParen,
    RParen,
    LBracket,
    RBracket,
    At,
    Plus,
    Minus,
    Star,
    Slash,
    Caret,
    /// What the parser sees past the last token. Never in a tree.
    Eof,

    // Nodes.
    File,
    SourceItem,
    ParamItem,
    LetItem,
    OutItem,
    /// `fn clean(s) = s | notch(60)`
    FnItem,
    /// `@rate(1000)`
    Attr,
    /// A unit, after a colon or an `in`, such as `nT`, `nT/V` or `m/s^2`.
    Unit,
    /// `[45 .. 65]`
    Range,
    /// `/0/field/x`
    Path,
    /// A number, with its sign where a declaration allows one.
    Literal,
    /// A use of a name.
    NameRef,
    Call,
    /// One argument, with its name if it has one.
    Arg,
    /// `input | call(…)`
    Pipeline,
    Binary,
    Negation,
    /// `signal.name`
    Select,
    /// `value in unit`
    Conversion,
    Paren,
    /// Tokens that fit nowhere.
    Error,
}

impl SyntaxKind {
    /// Whether the parser steps over this token.
    pub fn is_trivia(self) -> bool {
        matches!(self, Self::Whitespace | Self::Comment | Self::Unknown)
    }

    /// Whether this keyword begins a definition.
    pub fn starts_item(self) -> bool {
        matches!(
            self,
            Self::SourceKw | Self::ParamKw | Self::LetKw | Self::OutKw | Self::FnKw
        )
    }

    /// Whether a node of this kind is an expression.
    pub fn is_expr(self) -> bool {
        matches!(
            self,
            Self::NameRef
                | Self::Literal
                | Self::Path
                | Self::Call
                | Self::Pipeline
                | Self::Binary
                | Self::Negation
                | Self::Select
                | Self::Conversion
                | Self::Paren
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub kind: SyntaxKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntaxElement {
    Node(SyntaxNode),
    Token(Token),
}

/// A node of the tree. Except for the file itself, a node begins and ends at
/// a significant token, so its span is exactly the text of the construct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxNode {
    pub kind: SyntaxKind,
    pub span: Span,
    pub children: Vec<SyntaxElement>,
}

impl SyntaxNode {
    /// The child nodes, in order.
    pub fn nodes(&self) -> impl Iterator<Item = &SyntaxNode> {
        self.children.iter().filter_map(|child| match child {
            SyntaxElement::Node(node) => Some(node),
            SyntaxElement::Token(_) => None,
        })
    }

    /// The first child node of `kind`.
    pub fn node(&self, kind: SyntaxKind) -> Option<&SyntaxNode> {
        self.nodes().find(|node| node.kind == kind)
    }

    /// The tokens directly under this node that the parser did not skip.
    pub fn tokens(&self) -> impl Iterator<Item = Token> + '_ {
        self.children.iter().filter_map(|child| match child {
            SyntaxElement::Token(token) if !token.kind.is_trivia() => Some(*token),
            _ => None,
        })
    }

    /// The first token of `kind` directly under this node.
    pub fn token(&self, kind: SyntaxKind) -> Option<Token> {
        self.tokens().find(|token| token.kind == kind)
    }

    /// Every token under this node, skipped ones included, in source order.
    pub fn all_tokens(&self) -> Vec<Token> {
        fn walk(node: &SyntaxNode, out: &mut Vec<Token>) {
            for child in &node.children {
                match child {
                    SyntaxElement::Node(node) => walk(node, out),
                    SyntaxElement::Token(token) => out.push(*token),
                }
            }
        }
        let mut out = Vec::new();
        walk(self, &mut out);
        out
    }

    /// The source text this tree was parsed from, rebuilt from its tokens.
    pub fn text(&self, source: &str) -> String {
        self.all_tokens()
            .iter()
            .map(|token| token.span.text(source))
            .collect()
    }

    /// The innermost node whose span contains `offset`.
    pub fn node_at(&self, offset: usize) -> &SyntaxNode {
        self.nodes()
            .find(|node| node.span.start <= offset && offset < node.span.end)
            .map_or(self, |node| node.node_at(offset))
    }
}

//! A recursive-descent parser that builds the lossless syntax tree.
//!
//! ```text
//! file     := item*
//! item     := "source" name "=" path attr*
//!           | "param" name (":" unit)? "=" literal range?
//!           | "let" name "=" expr
//!           | "out" name (":" unit)? "=" expr
//!           | "fn" name "(" (name ("," name)*)? ")" "=" expr
//! attr     := "@" name "(" (arg ("," arg)*)? ")"
//! range    := "[" literal ".." literal "]"
//! literal  := "-"? quantity
//! quantity := number name?              `60`, `10s`, `10 s`
//! unit     := factor (("*" | "/") factor)*
//! factor   := (name | "1") ("^" "-"? number)?
//! expr     := sum ("|" call)* ("in" unit)?
//! sum      := product (("+" | "-") product)*
//! product  := unary (("*" | "/") unary)*
//! unary    := "-" unary | postfix
//! postfix  := primary ("." name)*
//! primary  := name | quantity | path | call | "(" expr ")"
//! call     := name "(" (arg ("," arg)*)? ")"
//! arg      := (name "=")? expr
//! path     := ("/" segment)+        no spaces; a segment may contain dots
//! ```
//!
//! A pipe binds looser than arithmetic, so `a - b | f()` filters the
//! difference, and a stage cannot be followed by an operator without
//! parentheses. `in` binds loosest of all and converts the whole expression.
//! Definitions need no terminator. Each begins with a keyword, and the
//! parser recovers from an error by skipping to the next one, so one broken
//! definition costs one diagnostic.

use super::diag::{code, Diagnostic};
use super::lexer::lex;
use super::syntax::{SyntaxElement, SyntaxKind, SyntaxNode, Token};
use super::Span;
use SyntaxKind::*;

/// A syntax tree and what was wrong with the text it came from.
pub struct Parse {
    pub root: SyntaxNode,
    pub diagnostics: Vec<Diagnostic>,
}

/// Parses `source`. There is always a tree, whatever the text.
pub fn parse(source: &str) -> Parse {
    let tokens = lex(source);
    let diagnostics = tokens
        .iter()
        .filter(|token| token.kind == Unknown)
        .map(|token| {
            let text = token.span.text(source);
            let noun = if text.chars().count() == 1 {
                "character"
            } else {
                "characters"
            };
            Diagnostic::error(
                code::UNKNOWN_CHARACTER,
                token.span,
                format!("unknown {noun} `{text}`"),
            )
        })
        .collect();
    let mut parser = Parser {
        source,
        tokens,
        position: 0,
        last_end: 0,
        stack: vec![Frame {
            kind: File,
            children: Vec::new(),
            start: 0,
        }],
        diagnostics,
    };
    parser.file();
    parser.eat_trivia();
    let frame = parser.stack.pop().expect("the file frame is still open");
    Parse {
        root: SyntaxNode {
            kind: File,
            span: Span::new(0, source.len()),
            children: frame.children,
        },
        diagnostics: parser.diagnostics,
    }
}

/// A node under construction.
struct Frame {
    kind: SyntaxKind,
    children: Vec<SyntaxElement>,
    start: usize,
}

/// The rest of the definition cannot be parsed. The error is already reported.
struct Stop;

type Parsed = Result<(), Stop>;

struct Parser<'s> {
    source: &'s str,
    tokens: Vec<Token>,
    position: usize,
    /// Where the last significant token ended.
    last_end: usize,
    stack: Vec<Frame>,
    diagnostics: Vec<Diagnostic>,
}

impl Parser<'_> {
    // Tokens.

    /// The `n`th token ahead that is not skipped.
    fn nth_token(&self, n: usize) -> Option<Token> {
        self.tokens[self.position..]
            .iter()
            .filter(|token| !token.kind.is_trivia())
            .nth(n)
            .copied()
    }

    fn nth(&self, n: usize) -> SyntaxKind {
        self.nth_token(n).map_or(Eof, |token| token.kind)
    }

    fn peek(&self) -> SyntaxKind {
        self.nth(0)
    }

    fn at(&self, kind: SyntaxKind) -> bool {
        self.peek() == kind
    }

    /// The very next token's kind, when nothing separates it from the last.
    fn adjacent(&self) -> SyntaxKind {
        self.tokens
            .get(self.position)
            .map_or(Eof, |token| token.kind)
    }

    fn at_item_boundary(&self) -> bool {
        self.peek() == Eof || self.peek().starts_item()
    }

    // Tree building.

    fn top(&mut self) -> &mut Frame {
        self.stack
            .last_mut()
            .expect("the file frame is always open")
    }

    fn eat_trivia(&mut self) {
        while let Some(token) = self.tokens.get(self.position).copied() {
            if !token.kind.is_trivia() {
                break;
            }
            self.top().children.push(SyntaxElement::Token(token));
            self.position += 1;
        }
    }

    fn offset(&self) -> usize {
        self.tokens
            .get(self.position)
            .map_or(self.source.len(), |token| token.span.start)
    }

    /// Opens a node at the next significant token.
    fn start(&mut self, kind: SyntaxKind) {
        self.eat_trivia();
        let start = self.offset();
        self.stack.push(Frame {
            kind,
            children: Vec::new(),
            start,
        });
    }

    /// Marks a place a node can later be opened at, around what follows.
    fn checkpoint(&mut self) -> usize {
        self.eat_trivia();
        self.top().children.len()
    }

    /// Opens a node around everything parsed since `checkpoint`.
    fn start_at(&mut self, checkpoint: usize, kind: SyntaxKind) {
        let children = self.top().children.split_off(checkpoint);
        let start = match children.first() {
            Some(SyntaxElement::Node(node)) => node.span.start,
            Some(SyntaxElement::Token(token)) => token.span.start,
            None => self.offset(),
        };
        self.stack.push(Frame {
            kind,
            children,
            start,
        });
    }

    fn finish(&mut self) {
        let frame = self.stack.pop().expect("a node is open");
        let end = match frame.children.last() {
            Some(SyntaxElement::Node(node)) => node.span.end,
            Some(SyntaxElement::Token(token)) => token.span.end,
            None => frame.start,
        };
        let node = SyntaxNode {
            kind: frame.kind,
            span: Span::new(frame.start, end),
            children: frame.children,
        };
        self.top().children.push(SyntaxElement::Node(node));
    }

    fn bump(&mut self) {
        self.eat_trivia();
        if let Some(token) = self.tokens.get(self.position).copied() {
            self.top().children.push(SyntaxElement::Token(token));
            self.position += 1;
            self.last_end = token.span.end;
        }
    }

    fn eat(&mut self, kind: SyntaxKind) -> bool {
        let found = self.at(kind);
        if found {
            self.bump();
        }
        found
    }

    fn expect(&mut self, kind: SyntaxKind, what: &str) -> Parsed {
        if self.eat(kind) {
            Ok(())
        } else {
            Err(self.unexpected(what))
        }
    }

    // Errors.

    /// Reports that `what` was expected here.
    fn unexpected(&mut self, what: &str) -> Stop {
        let diagnostic = match self.nth_token(0) {
            // The definition ran out. Point at where it stopped, not at the
            // next definition.
            Some(token) if token.kind.starts_item() => None,
            Some(token) => {
                let text = token.span.text(self.source);
                Some(Diagnostic::error(
                    code::SYNTAX,
                    token.span,
                    format!("expected {what}, found `{text}`"),
                ))
            }
            None => None,
        };
        self.diagnostics.push(diagnostic.unwrap_or_else(|| {
            Diagnostic::error(
                code::SYNTAX,
                Span::at(self.last_end),
                format!("expected {what}"),
            )
            .label("the definition stops here")
        }));
        Stop
    }

    /// Wraps everything up to the next definition in an error node.
    fn skip_to_item(&mut self) {
        if self.at_item_boundary() {
            return;
        }
        self.start(Error);
        while !self.at_item_boundary() {
            self.bump();
        }
        self.finish();
    }

    // Grammar.

    fn file(&mut self) {
        let mut first = true;
        loop {
            match self.peek() {
                Eof => break,
                SourceKw => self.item(SourceItem, Self::source),
                ParamKw => self.item(ParamItem, Self::param),
                LetKw => self.item(LetItem, Self::binding),
                OutKw => self.item(OutItem, Self::output),
                FnKw => self.item(FnItem, Self::function),
                _ => {
                    // Text between definitions. After a definition it may be
                    // a continuation that lost its operator, so say both. The
                    // definition before it is complete and stays as it is.
                    self.unexpected(if first {
                        "a definition"
                    } else {
                        "an operator or a new definition"
                    });
                    if let Some(diagnostic) = self.diagnostics.pop() {
                        self.diagnostics.push(diagnostic.help(
                            "a definition begins with `source`, `param`, `let`, `out` or `fn`",
                        ));
                    }
                    self.skip_to_item();
                }
            }
            first = false;
        }
    }

    fn item(&mut self, kind: SyntaxKind, body: fn(&mut Self) -> Parsed) {
        self.start(kind);
        let depth = self.stack.len();
        self.bump();
        if body(self).is_err() {
            // An early exit leaves nodes open. Close them, innermost first.
            while self.stack.len() > depth {
                self.finish();
            }
            self.mark_broken();
        }
        self.finish();
    }

    /// Ends a definition that failed to parse with an error node holding
    /// whatever is left of it, so that later stages know not to trust it.
    fn mark_broken(&mut self) {
        if self.at_item_boundary() {
            // Nothing is left. The node is empty and sits where the
            // definition stopped, ahead of any comment that follows.
            let node = SyntaxNode {
                kind: Error,
                span: Span::at(self.last_end),
                children: Vec::new(),
            };
            self.top().children.push(SyntaxElement::Node(node));
        } else {
            self.skip_to_item();
        }
    }

    fn source(&mut self) -> Parsed {
        self.expect(Ident, "a name for the source")?;
        self.expect(Eq, "`=`")?;
        if !self.at(Slash) {
            return Err(self.unexpected("a stream path such as `/0/field`"));
        }
        self.path()?;
        while self.at(At) {
            self.attr()?;
        }
        Ok(())
    }

    fn param(&mut self) -> Parsed {
        self.expect(Ident, "a name for the param")?;
        if self.eat(Colon) {
            self.unit()?;
        }
        self.expect(Eq, "`=`")?;
        self.literal()?;
        if self.at(LBracket) {
            self.start(Range);
            self.bump();
            self.literal()?;
            self.expect(DotDot, "`..`")?;
            self.literal()?;
            self.expect(RBracket, "`]`")?;
            self.finish();
        }
        Ok(())
    }

    fn binding(&mut self) -> Parsed {
        self.expect(Ident, "a name")?;
        self.expect(Eq, "`=`")?;
        self.expr()
    }

    fn output(&mut self) -> Parsed {
        self.expect(Ident, "a name for the output")?;
        if self.eat(Colon) {
            self.unit()?;
        }
        self.expect(Eq, "`=`")?;
        self.expr()
    }

    fn function(&mut self) -> Parsed {
        self.expect(Ident, "a name for the function")?;
        self.expect(LParen, "`(`")?;
        if !self.at(RParen) {
            loop {
                self.expect(Ident, "a name for what the function is given")?;
                if !self.eat(Comma) {
                    break;
                }
            }
        }
        self.expect(RParen, "`)`")?;
        self.expect(Eq, "`=`")?;
        self.expr()
    }

    /// A unit, which is symbols joined by `*` and `/`, each with an optional
    /// power.
    fn unit(&mut self) -> Parsed {
        self.start(Unit);
        loop {
            // A symbol, or the `1` of `1/s`.
            if !matches!(self.peek(), Ident | Number) {
                return Err(self.unexpected("a unit"));
            }
            self.bump();
            if self.eat(Caret) {
                self.eat(Minus);
                self.expect(Number, "a whole power")?;
            }
            if !matches!(self.peek(), Star | Slash) {
                break;
            }
            self.bump();
        }
        self.finish();
        Ok(())
    }

    /// A number, and the unit after it if there is one, as in `60`, `10s`
    /// or `10 s`.
    fn quantity(&mut self) -> Parsed {
        self.expect(Number, "a number")?;
        if self.at(Ident) && !matches!(self.nth(1), LParen | Eq) {
            self.bump();
        }
        Ok(())
    }

    fn literal(&mut self) -> Parsed {
        self.start(Literal);
        self.eat(Minus);
        self.quantity()?;
        self.finish();
        Ok(())
    }

    fn attr(&mut self) -> Parsed {
        self.start(Attr);
        self.bump();
        self.expect(Ident, "an attribute name")?;
        self.expect(LParen, "`(`")?;
        self.args()?;
        self.finish();
        Ok(())
    }

    /// The arguments of a call or an attribute, after the opening parenthesis.
    fn args(&mut self) -> Parsed {
        while !self.at(RParen) {
            self.start(Arg);
            if self.at(Ident) && self.nth(1) == Eq {
                self.bump();
                self.bump();
            }
            self.expr()?;
            self.finish();
            if !self.eat(Comma) {
                break;
            }
        }
        self.expect(RParen, "`,` or `)`")
    }

    /// A stream path. Its parts touch, and a segment runs to the next `/` or
    /// to the first token that cannot be part of a name.
    fn path(&mut self) -> Parsed {
        self.start(Path);
        loop {
            self.bump();
            let mut any = false;
            while matches!(self.adjacent(), Ident | Number | Dot | Minus)
                || self.adjacent().starts_item()
            {
                self.bump();
                any = true;
            }
            if !any {
                self.diagnostics.push(
                    Diagnostic::error(
                        code::STREAM_PATH,
                        Span::at(self.last_end),
                        "a stream path names something after each `/`",
                    )
                    .help("write a path without spaces, such as `/0/field` or `/0/field/x`"),
                );
                return Err(Stop);
            }
            if self.adjacent() != Slash {
                break;
            }
        }
        self.finish();
        Ok(())
    }

    fn expr(&mut self) -> Parsed {
        let checkpoint = self.checkpoint();
        self.sum()?;
        let mut piped = false;
        while self.at(Pipe) {
            self.start_at(checkpoint, Pipeline);
            self.bump();
            if !(self.at(Ident) && self.nth(1) == LParen) {
                if let (Ident, Some(name)) = (self.peek(), self.nth_token(0)) {
                    let text = name.span.text(self.source);
                    self.diagnostics.push(
                        Diagnostic::error(
                            code::SYNTAX,
                            name.span,
                            "a pipeline stage is a function call",
                        )
                        .label(format!("write `{text}(…)`")),
                    );
                    return Err(Stop);
                }
                return Err(self.unexpected("a function call after `|`"));
            }
            self.call()?;
            self.finish();
            piped = true;
        }
        if piped && matches!(self.peek(), Plus | Minus | Star | Slash) {
            let operator = self.nth_token(0).expect("an operator is next");
            self.diagnostics.push(
                Diagnostic::error(
                    code::SYNTAX,
                    operator.span,
                    "an operator cannot follow a pipeline stage",
                )
                .help("put the pipeline in parentheses: `(x | f()) - y`"),
            );
            return Err(Stop);
        }
        if self.at(InKw) {
            self.start_at(checkpoint, Conversion);
            self.bump();
            self.unit()?;
            self.finish();
        }
        Ok(())
    }

    fn sum(&mut self) -> Parsed {
        let checkpoint = self.checkpoint();
        self.product()?;
        while matches!(self.peek(), Plus | Minus) {
            self.start_at(checkpoint, Binary);
            self.bump();
            self.product()?;
            self.finish();
        }
        Ok(())
    }

    fn product(&mut self) -> Parsed {
        let checkpoint = self.checkpoint();
        self.unary()?;
        while matches!(self.peek(), Star | Slash) {
            self.start_at(checkpoint, Binary);
            self.bump();
            self.unary()?;
            self.finish();
        }
        Ok(())
    }

    fn unary(&mut self) -> Parsed {
        if self.at(Minus) {
            self.start(Negation);
            self.bump();
            self.unary()?;
            self.finish();
            return Ok(());
        }
        let checkpoint = self.checkpoint();
        self.primary()?;
        while self.at(Dot) {
            self.start_at(checkpoint, Select);
            self.bump();
            self.expect(Ident, "a channel or group name")?;
            self.finish();
        }
        Ok(())
    }

    fn primary(&mut self) -> Parsed {
        match self.peek() {
            Ident if self.nth(1) == LParen => self.call(),
            Ident => {
                self.start(NameRef);
                self.bump();
                self.finish();
                Ok(())
            }
            Number => {
                self.start(Literal);
                self.quantity()?;
                self.finish();
                Ok(())
            }
            Slash => self.path(),
            LParen => {
                self.start(Paren);
                self.bump();
                self.expr()?;
                self.expect(RParen, "`)`")?;
                self.finish();
                Ok(())
            }
            _ => Err(self.unexpected("an expression")),
        }
    }

    fn call(&mut self) -> Parsed {
        self.start(Call);
        self.bump();
        self.bump();
        self.args()?;
        self.finish();
        Ok(())
    }
}

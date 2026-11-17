//! Cuts source text into tokens. Every byte lands in exactly one token.

use super::syntax::{SyntaxKind, Token};
use super::Span;

/// Lexes `source`. The tokens are contiguous and cover all of it.
pub fn lex(source: &str) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut offset = 0;
    while offset < source.len() {
        let rest = &source[offset..];
        let (kind, len) = next(rest);
        // A run of unknown characters is one token, and so one diagnostic.
        match tokens.last_mut() {
            Some(last) if kind == SyntaxKind::Unknown && last.kind == SyntaxKind::Unknown => {
                last.span.end += len;
            }
            _ => tokens.push(Token {
                kind,
                span: Span::new(offset, offset + len),
            }),
        }
        offset += len;
    }
    tokens
}

/// The kind and byte length of the token at the start of `rest`.
fn next(rest: &str) -> (SyntaxKind, usize) {
    use SyntaxKind::*;
    let first = rest.chars().next().expect("rest is not empty");
    let run = |keep: fn(char) -> bool| rest.find(|c| !keep(c)).unwrap_or(rest.len());

    if first.is_whitespace() {
        return (Whitespace, run(char::is_whitespace));
    }
    if first == '#' {
        return (Comment, rest.find('\n').unwrap_or(rest.len()));
    }
    if is_ident_start(first) {
        let len = run(is_ident_continue);
        let kind = match &rest[..len] {
            "source" => SourceKw,
            "param" => ParamKw,
            "let" => LetKw,
            "out" => OutKw,
            "fn" => FnKw,
            "in" => InKw,
            _ => Ident,
        };
        return (kind, len);
    }
    if first.is_ascii_digit() {
        let numeric = numeric_len(rest);
        let unit = rest[numeric..]
            .find(|c| !is_ident_continue(c))
            .unwrap_or(rest.len() - numeric);
        return (Number, numeric + unit);
    }
    if rest.starts_with("..") {
        return (DotDot, 2);
    }
    let kind = match first {
        '=' => Eq,
        '|' => Pipe,
        '.' => Dot,
        ',' => Comma,
        ':' => Colon,
        '(' => LParen,
        ')' => RParen,
        '[' => LBracket,
        ']' => RBracket,
        '@' => At,
        '+' => Plus,
        '-' => Minus,
        '*' => Star,
        '/' => Slash,
        '^' => Caret,
        _ => Unknown,
    };
    (kind, first.len_utf8())
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// How many bytes at the start of `text` are the number itself. That is
/// digits, an optional fraction and an optional exponent. What follows is
/// the unit.
fn numeric_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    // Digits, with `_` allowed between them to group them.
    let digits = |from: usize| {
        let run = bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_digit() || **b == b'_')
            .count();
        let ends_in_digit = |len: usize| len > 0 && bytes[from + len - 1].is_ascii_digit();
        (0..=run)
            .rev()
            .find(|len| *len == 0 || ends_in_digit(*len))
            .unwrap_or(0)
    };
    let mut len = digits(0);
    // A fraction needs a digit after the point, so that `45..65` is a range.
    if bytes.get(len) == Some(&b'.') && digits(len + 1) > 0 {
        len += 1 + digits(len + 1);
    }
    if matches!(bytes.get(len), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(len + 1), Some(b'+' | b'-')));
        let exponent = digits(len + 1 + sign);
        if exponent > 0 {
            len += 1 + sign + exponent;
        }
    }
    len
}

/// Splits a number token into its value and its unit suffix, if it has one.
pub fn split_number(text: &str) -> (f64, Option<&str>) {
    let (number, unit) = text.split_at(numeric_len(text));
    let value = number.replace('_', "").parse().unwrap_or(f64::NAN);
    (value, (!unit.is_empty()).then_some(unit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use SyntaxKind::*;

    fn kinds(source: &str) -> Vec<(SyntaxKind, &str)> {
        lex(source)
            .into_iter()
            .filter(|t| t.kind != Whitespace)
            .map(|t| (t.kind, t.span.text(source)))
            .collect()
    }

    #[test]
    fn numbers_carry_their_units_and_leave_ranges_alone() {
        assert_eq!(split_number("60"), (60.0, None));
        assert_eq!(split_number("0.1"), (0.1, None));
        assert_eq!(split_number("10s"), (10.0, Some("s")));
        assert_eq!(split_number("1e3"), (1000.0, None));
        assert_eq!(split_number("2.5e-3s"), (0.0025, Some("s")));
        assert_eq!(split_number("1.5kHz"), (1.5, Some("kHz")));
        // No digit after the `e`, so it is a unit.
        assert_eq!(split_number("2eV"), (2.0, Some("eV")));
        assert_eq!(split_number("44_100"), (44_100.0, None));
        assert_eq!(split_number("1_000.5ms"), (1000.5, Some("ms")));
        assert_eq!(
            kinds("[45..65]"),
            [
                (LBracket, "["),
                (Number, "45"),
                (DotDot, ".."),
                (Number, "65"),
                (RBracket, "]")
            ]
        );
    }

    #[test]
    fn every_byte_lands_in_one_token() {
        let source = "let µ = a | notch(60) # é\n\t$$ out x = /0/field/sync.vco.x\r\n";
        let tokens = lex(source);
        let mut at = 0;
        for token in &tokens {
            assert_eq!(token.span.start, at);
            at = token.span.end;
        }
        assert_eq!(at, source.len());
        assert!(kinds(source).contains(&(Unknown, "$$")));
        assert!(kinds(source).contains(&(Ident, "µ")));
        assert!(kinds(source).contains(&(Comment, "# é")));
    }
}

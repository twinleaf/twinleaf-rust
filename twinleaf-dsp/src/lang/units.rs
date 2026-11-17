//! Units, after the calcium text calculator.
//!
//! A unit is a product of symbols with whole powers, kept as it was written.
//! `nT/V` is `nT` to the first and `V` to the minus first, and stays that way.
//! Multiplying and dividing collect powers, so `V * nT/V` is `nT`. Nothing
//! is expanded unless a conversion asks, and then each symbol is looked up in
//! a small table of what it is worth in base units, with an SI prefix taken
//! off first if that is what makes it a known unit. A symbol the table does
//! not know is a unit of its own.
//!
//! A conversion never happens unasked. Two units that measure the same thing
//! at different scales are different units until an `in` says otherwise.

use crate::signal::Unit;
use std::collections::BTreeMap;
use std::fmt;

/// A product of unit symbols with whole powers. No symbols is no unit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnitExpr {
    powers: BTreeMap<String, i32>,
}

impl UnitExpr {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn symbol(name: &str) -> Self {
        Self::none().with(name, 1)
    }

    pub fn is_none(&self) -> bool {
        self.powers.is_empty()
    }

    fn with(mut self, symbol: &str, power: i32) -> Self {
        let total = self.powers.get(symbol).copied().unwrap_or(0) + power;
        if total == 0 {
            self.powers.remove(symbol);
        } else {
            self.powers.insert(symbol.to_owned(), total);
        }
        self
    }

    pub fn times(&self, other: &Self) -> Self {
        other
            .powers
            .iter()
            .fold(self.clone(), |unit, (symbol, power)| {
                unit.with(symbol, *power)
            })
    }

    pub fn over(&self, other: &Self) -> Self {
        self.times(&other.pow(-1))
    }

    pub fn pow(&self, exponent: i32) -> Self {
        Self {
            powers: self
                .powers
                .iter()
                .filter(|_| exponent != 0)
                .map(|(symbol, power)| (symbol.clone(), power * exponent))
                .collect(),
        }
    }

    /// Reads a unit as written. That is symbols joined by `*` and `/`, each
    /// with an optional `^` and a whole power, and `1` for an empty
    /// numerator, as in `nT/V`, `m/s^2` and `1/s`. Spaces are ignored.
    pub fn parse(text: &str) -> Option<Self> {
        let text: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        if text.is_empty() {
            return Some(Self::none());
        }
        let mut unit = Self::none();
        let mut sign = 1;
        let mut rest = text.as_str();
        loop {
            let end = rest.find(['*', '/']).unwrap_or(rest.len());
            let (factor, after) = rest.split_at(end);
            let (symbol, power) = match factor.split_once('^') {
                Some((symbol, power)) => (symbol, power.parse::<i32>().ok()?),
                None => (factor, 1),
            };
            let valid = !symbol.is_empty() && !symbol.contains(['^', '(', ')', ',']);
            if !valid {
                return None;
            }
            if symbol != "1" {
                if symbol.starts_with(|c: char| c.is_ascii_digit()) {
                    return None;
                }
                unit = unit.with(symbol, sign * power);
            }
            match after.chars().next() {
                None => return Some(unit),
                Some(operator) => {
                    sign = if operator == '/' { -1 } else { 1 };
                    rest = &after[1..];
                }
            }
        }
    }

    /// Reads the unit a channel or a number is labelled with. A label that
    /// does not read as a product is taken whole, as one symbol.
    pub fn of(unit: &Unit) -> Self {
        Self::parse(unit.symbol()).unwrap_or_else(|| Self::symbol(unit.symbol()))
    }

    /// The label for this unit, in its one spelling.
    pub fn label(&self) -> Unit {
        Unit::new(self.to_string())
    }

    /// What the unit is worth, as a factor and the base units it is a
    /// product of. The factor's power of ten is kept apart, so that a change
    /// of prefix is an exact power of ten and not a quotient of two rounded
    /// ones.
    fn expand(&self) -> (f64, i32, Self) {
        let mut mantissa = 1.0;
        let mut exponent = 0;
        let mut base = Self::none();
        for (symbol, power) in &self.powers {
            let (worth, tens, of) = resolve(symbol);
            mantissa *= worth.powi(*power);
            exponent += tens * power;
            base = base.times(&of.pow(*power));
        }
        (mantissa, exponent, base)
    }

    /// The factor that takes a number in this unit to one in `to`.
    pub fn convert(&self, to: &Self) -> Result<f64, Inconvertible> {
        let (from_mantissa, from_tens, from_base) = self.expand();
        let (to_mantissa, to_tens, to_base) = to.expand();
        if from_base != to_base {
            return Err(Inconvertible {
                from: self.clone(),
                to: to.clone(),
            });
        }
        let mantissa = from_mantissa / to_mantissa;
        let tens = from_tens - to_tens;
        Ok(if tens >= 0 {
            mantissa * 10f64.powi(tens)
        } else {
            mantissa / 10f64.powi(-tens)
        })
    }
}

/// Symbols in the numerator joined by `*`, then each in the denominator
/// after a `/`, with no spaces, as in `kg*m/s^2` and `1/s`.
impl fmt::Display for UnitExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let factor = |symbol: &str, power: i32| {
            if power == 1 {
                symbol.to_owned()
            } else {
                format!("{symbol}^{power}")
            }
        };
        let over: Vec<String> = self
            .powers
            .iter()
            .filter(|(_, power)| **power > 0)
            .map(|(symbol, power)| factor(symbol, *power))
            .collect();
        let under = self.powers.iter().filter(|(_, power)| **power < 0);
        if over.is_empty() && !self.is_none() {
            f.write_str("1")?;
        }
        f.write_str(&over.join("*"))?;
        for (symbol, power) in under {
            write!(f, "/{}", factor(symbol, -power))?;
        }
        Ok(())
    }
}

/// Two units that do not measure the same thing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inconvertible {
    pub from: UnitExpr,
    pub to: UnitExpr,
}

impl fmt::Display for Inconvertible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = |unit: &UnitExpr| {
            if unit.is_none() {
                "a plain number".to_owned()
            } else {
                unit.to_string()
            }
        };
        write!(
            f,
            "cannot convert {} to {}",
            name(&self.from),
            name(&self.to)
        )
    }
}

/// Whether `symbol` is a unit the table knows, with or without a prefix.
pub fn is_known(symbol: &str) -> bool {
    defined(symbol).is_some() || prefixed(symbol).is_some()
}

/// What a symbol is worth in base units, as a factor times a power of ten.
/// One the table does not know is its own base unit.
fn resolve(symbol: &str) -> (f64, i32, UnitExpr) {
    let base = |text: &str| UnitExpr::parse(text).expect("the table is well formed");
    if let Some((factor, tens, of)) = defined(symbol) {
        return (factor, tens, base(of));
    }
    if let Some((prefix, (factor, tens, of))) = prefixed(symbol) {
        return (factor, prefix + tens, base(of));
    }
    (1.0, 0, UnitExpr::symbol(symbol))
}

/// A table unit with an SI prefix in front. The first prefix that leaves a
/// table unit wins.
fn prefixed(symbol: &str) -> Option<(i32, (f64, i32, &'static str))> {
    PREFIXES.iter().find_map(|(prefix, tens)| {
        let unit = defined(symbol.strip_prefix(prefix)?)?;
        Some((*tens, unit))
    })
}

/// Each prefix and its power of ten.
const PREFIXES: &[(&str, i32)] = &[
    ("da", 1),
    ("Y", 24),
    ("Z", 21),
    ("E", 18),
    ("P", 15),
    ("T", 12),
    ("G", 9),
    ("M", 6),
    ("k", 3),
    ("h", 2),
    ("d", -1),
    ("c", -2),
    ("m", -3),
    ("u", -6),
    ("µ", -6),
    ("μ", -6),
    ("n", -9),
    ("p", -12),
    ("f", -15),
    ("a", -18),
    ("z", -21),
    ("y", -24),
];

/// The base units, which are defined as themselves, and the units defined in
/// terms of them. Each is a factor, a power of ten, and a product of base
/// units.
fn defined(symbol: &str) -> Option<(f64, i32, &'static str)> {
    Some(match symbol {
        "s" => (1.0, 0, "s"),
        "m" => (1.0, 0, "m"),
        "kg" => (1.0, 0, "kg"),
        "A" => (1.0, 0, "A"),
        "K" => (1.0, 0, "K"),
        "mol" => (1.0, 0, "mol"),
        "rad" => (1.0, 0, "rad"),
        "g" => (1.0, -3, "kg"),
        "min" => (60.0, 0, "s"),
        "hr" => (3600.0, 0, "s"),
        "day" => (86_400.0, 0, "s"),
        "Hz" => (1.0, 0, "1/s"),
        "N" => (1.0, 0, "kg*m/s^2"),
        "Pa" => (1.0, 0, "kg/m/s^2"),
        "J" => (1.0, 0, "kg*m^2/s^2"),
        "W" => (1.0, 0, "kg*m^2/s^3"),
        "C" => (1.0, 0, "A*s"),
        "V" => (1.0, 0, "kg*m^2/A/s^3"),
        "ohm" | "Ω" => (1.0, 0, "kg*m^2/A^2/s^3"),
        "F" => (1.0, 0, "A^2*s^4/kg/m^2"),
        "Wb" => (1.0, 0, "kg*m^2/A/s^2"),
        "T" => (1.0, 0, "kg/A/s^2"),
        "H" => (1.0, 0, "kg*m^2/A^2/s^2"),
        "gauss" => (1.0, -4, "kg/A/s^2"),
        "deg" | "°" => (std::f64::consts::PI / 180.0, 0, "rad"),
        "%" => (1.0, -2, ""),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(text: &str) -> UnitExpr {
        UnitExpr::parse(text).unwrap()
    }

    fn convert(from: &str, to: &str) -> Result<f64, String> {
        unit(from).convert(&unit(to)).map_err(|e| e.to_string())
    }

    #[test]
    fn units_multiply_and_divide_by_collecting_powers() {
        // 3 nT/V * 2 V is 6 nT.
        assert_eq!(unit("nT/V").times(&unit("V")), unit("nT"));
        assert_eq!(unit("m").over(&unit("s^2")).to_string(), "m/s^2");
        assert_eq!(unit("V").pow(2).to_string(), "V^2");
        assert_eq!(unit("m^2").over(&unit("m")), unit("m"));
        assert_eq!(unit("nT").over(&unit("nT")), UnitExpr::none());
        // Different spellings of one scale are not cancelled unasked.
        assert_eq!(unit("kHz").times(&unit("ms")).to_string(), "kHz*ms");
    }

    #[test]
    fn a_unit_prints_as_it_reads() {
        for text in [
            "nT", "nT/V", "m/s^2", "kg*m/s^2", "1/s", "N*m", "m/V/s", "V^2",
        ] {
            assert_eq!(unit(text).to_string(), text);
        }
        assert_eq!(unit(" kg * m / s^2 ").to_string(), "kg*m/s^2");
        assert_eq!(unit("s^-1").to_string(), "1/s");
        assert_eq!(unit("").to_string(), "");
        for text in ["nT/", "/s", "m^x", "2m", "a(b)", "m**s"] {
            assert_eq!(UnitExpr::parse(text), None, "{text}");
        }
    }

    #[test]
    fn conversion_goes_through_base_units_and_prefixes() {
        assert_eq!(convert("kHz", "Hz"), Ok(1e3));
        assert_eq!(convert("ms", "s"), Ok(1e-3));
        assert_eq!(convert("min", "s"), Ok(60.0));
        // A change of prefix is an exact power of ten.
        assert_eq!(convert("gauss", "nT"), Ok(1e5));
        assert_eq!(convert("µT", "nT"), Ok(1e3));
        assert_eq!(convert("nT", "µT"), Ok(1e-3));
        assert_eq!(convert("uT", "µT"), Ok(1.0));
        // One over a time is a frequency.
        assert_eq!(convert("1/ms", "Hz"), Ok(1e3));
        assert_eq!(convert("1/s", "Hz"), Ok(1.0));
        assert_eq!(convert("V*s", "Wb"), Ok(1.0));
        assert_eq!(convert("nT/V", "T/mV"), Ok(1e-12));
        // `min` and `m` are units themselves before they are a prefix.
        assert_eq!(convert("mm", "m"), Ok(1e-3));
        assert_eq!(convert("%", ""), Ok(0.01));
    }

    #[test]
    fn units_of_different_things_do_not_convert() {
        assert_eq!(convert("m", "s"), Err("cannot convert m to s".into()));
        assert_eq!(convert("nT", "V"), Err("cannot convert nT to V".into()));
        assert_eq!(convert("s", "Hz"), Err("cannot convert s to Hz".into()));
        assert_eq!(
            convert("nT", ""),
            Err("cannot convert nT to a plain number".into())
        );
        // A symbol nobody defined is a unit of its own.
        assert_eq!(convert("counts", "counts"), Ok(1.0));
        assert_eq!(
            convert("counts", "V"),
            Err("cannot convert counts to V".into())
        );
        assert!(is_known("nT") && is_known("Hz") && is_known("ms"));
        assert!(!is_known("counts") && !is_known("clean"));
    }
}

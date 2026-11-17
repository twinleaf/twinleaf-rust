//! The functions a spec can call, with what each takes and how its result
//! is typed. This is the language's half of the node library. The signal
//! processing behind each name is in [`crate::nodes`], and [`crate::lower`]
//! joins the two.

/// How many signals a function takes ahead of its other arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signals {
    /// One, usually piped in.
    One,
    /// Two or more, each an arm with a name.
    Arms,
}

/// What an argument is, which decides what may be written for it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArgKind {
    /// A frequency within the signal's band. May be a param, so it can be
    /// tuned while the graph runs.
    Frequency,
    /// A duration. May be a param.
    Time,
    /// A positive number without a unit. May be a param.
    Number,
    /// A sample rate, written out, because it fixes the shape of the graph.
    Rate,
    /// A duration, written out for the same reason.
    Span,
    /// A whole number of at least `min`, written out for the same reason.
    Integer { min: i64 },
    /// One of a fixed set of words.
    Choice(&'static [&'static str]),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArgSpec {
    pub name: &'static str,
    pub kind: ArgKind,
    pub required: bool,
}

/// An argument a call must give.
pub const fn required(name: &'static str, kind: ArgKind) -> ArgSpec {
    ArgSpec {
        name,
        kind,
        required: true,
    }
}

/// An argument a call may leave out, for the function to supply.
pub const fn optional(name: &'static str, kind: ArgKind) -> ArgSpec {
    ArgSpec {
        name,
        kind,
        required: false,
    }
}

/// How the type of a call's result follows from its inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// The input's type, unchanged.
    Same,
    /// The input's channels, each in its unit times seconds.
    Integrates,
    /// The input's channels, each in its unit per second.
    Differentiates,
    /// One channel, in the unit the input's channels share.
    Magnitude,
    /// A new domain at the input's rate divided by `n`.
    Decimate,
    /// A new domain at `rate`.
    Resample,
    /// One new domain holding every arm's channels, named `arm.channel`.
    Align,
    /// The first signal's type. The reference must share its domain.
    Decorrelate,
    /// The input's channels, each in its unit squared per hertz, over
    /// frequency instead of time.
    Spectrum,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Builtin {
    pub name: &'static str,
    /// How a call is written, defaults included.
    pub usage: &'static str,
    pub signals: Signals,
    /// A second signal passed by name, such as `ref`.
    pub named_signal: Option<&'static str>,
    pub args: &'static [ArgSpec],
    pub rule: Rule,
}

const ORDER: ArgKind = ArgKind::Integer { min: 1 };

/// Whether a filter runs forwards only, as it must while samples are still
/// arriving, or both ways over a whole run, which leaves no delay.
const PHASE: ArgKind = ArgKind::Choice(&["causal", "zero"]);

pub static BUILTINS: &[Builtin] = &[
    Builtin {
        name: "align",
        usage: "align(a, b, …)",
        signals: Signals::Arms,
        named_signal: None,
        args: &[],
        rule: Rule::Align,
    },
    Builtin {
        name: "decimate",
        usage: "decimate(n)",
        signals: Signals::One,
        named_signal: None,
        args: &[required("n", ArgKind::Integer { min: 2 })],
        rule: Rule::Decimate,
    },
    Builtin {
        name: "resample",
        usage: "resample(rate)",
        signals: Signals::One,
        named_signal: None,
        args: &[required("rate", ArgKind::Rate)],
        rule: Rule::Resample,
    },
    Builtin {
        name: "notch",
        usage: "notch(f, q = 30, harmonics = 1, phase = causal)",
        signals: Signals::One,
        named_signal: None,
        args: &[
            required("f", ArgKind::Frequency),
            optional("q", ArgKind::Number),
            optional("harmonics", ORDER),
            optional("phase", PHASE),
        ],
        rule: Rule::Same,
    },
    Builtin {
        name: "bandpass",
        usage: "bandpass(lo, hi, order = 4, phase = causal)",
        signals: Signals::One,
        named_signal: None,
        args: &[
            required("lo", ArgKind::Frequency),
            required("hi", ArgKind::Frequency),
            optional("order", ORDER),
            optional("phase", PHASE),
        ],
        rule: Rule::Same,
    },
    Builtin {
        name: "lowpass",
        usage: "lowpass(f, order = 4, phase = causal)",
        signals: Signals::One,
        named_signal: None,
        args: &[
            required("f", ArgKind::Frequency),
            optional("order", ORDER),
            optional("phase", PHASE),
        ],
        rule: Rule::Same,
    },
    Builtin {
        name: "highpass",
        usage: "highpass(f, order = 4, phase = causal)",
        signals: Signals::One,
        named_signal: None,
        args: &[
            required("f", ArgKind::Frequency),
            optional("order", ORDER),
            optional("phase", PHASE),
        ],
        rule: Rule::Same,
    },
    Builtin {
        name: "detrend",
        usage: "detrend(mode)",
        signals: Signals::One,
        named_signal: None,
        args: &[required(
            "mode",
            ArgKind::Choice(&["mean", "linear", "quadratic"]),
        )],
        rule: Rule::Same,
    },
    Builtin {
        name: "magnitude",
        usage: "magnitude(v)",
        signals: Signals::One,
        named_signal: None,
        args: &[],
        rule: Rule::Magnitude,
    },
    Builtin {
        name: "decorrelate",
        usage: "decorrelate(x, ref = …, window = …, method = ls, taps = 1)",
        signals: Signals::One,
        named_signal: Some("ref"),
        args: &[
            required("window", ArgKind::Time),
            optional("method", ArgKind::Choice(&["ls", "nlms", "rls"])),
            optional("taps", ORDER),
        ],
        rule: Rule::Decorrelate,
    },
    Builtin {
        name: "psd",
        usage: "psd(window)",
        signals: Signals::One,
        named_signal: None,
        args: &[required("window", ArgKind::Span)],
        rule: Rule::Spectrum,
    },
    Builtin {
        name: "rms",
        usage: "rms(window)",
        signals: Signals::One,
        named_signal: None,
        args: &[required("window", ArgKind::Time)],
        rule: Rule::Same,
    },
    Builtin {
        name: "integrate",
        usage: "integrate()",
        signals: Signals::One,
        named_signal: None,
        args: &[],
        rule: Rule::Integrates,
    },
    Builtin {
        name: "derivative",
        usage: "derivative()",
        signals: Signals::One,
        named_signal: None,
        args: &[],
        rule: Rule::Differentiates,
    },
];

/// Returns the function called `name`.
pub fn find(name: &str) -> Option<&'static Builtin> {
    BUILTINS.iter().find(|builtin| builtin.name == name)
}

/// The name in `candidates` that `name` was most likely meant to be.
pub fn closest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    candidates
        .into_iter()
        .map(|candidate| (distance(name, candidate), candidate))
        .filter(|(distance, _)| {
            // Near enough to be a slip, and not a different name altogether.
            let length = name.chars().count();
            *distance <= ((length + 1) / 3).max(1) && *distance < length
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, candidate)| candidate)
}

/// Edit distance, counting insertions, deletions and substitutions.
fn distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, y) in b.iter().enumerate() {
            let substitute = diagonal + usize::from(x != *y);
            diagonal = row[j + 1];
            row[j + 1] = substitute.min(row[j] + 1).min(diagonal + 1);
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_closest_name_is_a_near_miss_and_nothing_else() {
        let names = || BUILTINS.iter().map(|b| b.name);
        assert_eq!(closest("nothc", names()), Some("notch"));
        assert_eq!(closest("bandpas", names()), Some("bandpass"));
        assert_eq!(closest("lowpas", names()), Some("lowpass"));
        assert_eq!(closest("fourier", names()), None);
        assert_eq!(closest("claen", ["a", "clean"]), Some("clean"));
        // A one-letter name is not a misspelling of another.
        assert_eq!(closest("b", ["a"]), None);
        assert_eq!(distance("kitten", "sitting"), 3);
    }
}

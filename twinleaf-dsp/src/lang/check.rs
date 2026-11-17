//! Resolves names, types every expression, and checks domains and units.
//!
//! The result is a [`Program`], the spec as a graph of typed nodes, which is
//! what a figure is drawn from and what will be lowered into a running graph.
//!
//! Three rules carry most of the weight.
//!
//! - **Domains.** Every signal lives in a rate domain. A source establishes
//!   one per stream, `decimate` and `resample` derive one, and `align` merges
//!   its arms into a new one. Nothing else crosses domains, so combining
//!   signals from two of them is an error rather than an interpolation.
//! - **Names.** Channels pair by name, never by position. `align` names each
//!   arm's channels `arm.channel`, and `signal.arm` takes them back out.
//! - **Units.** A unit is a label. Where both sides have one they must agree,
//!   and an operation that changes the dimension drops the label rather than
//!   computing a new one.
//!
//! A definition that fails is bound as unknown, and whatever uses it is
//! skipped silently, so one mistake is reported once.

use super::ast::{self, BinaryOp, Expr};
use super::builtins::{self, ArgKind, ArgSpec, Builtin, Rule, Signals};
use super::diag::{code, Diagnostic};
use super::units::{self, UnitExpr};
use super::Span;
use crate::library::{remembered, Library};
use crate::signal::{Channel, ChannelSet, DomainId, SignalType, Unit};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

/// A checked spec.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Program {
    /// The functions the program was checked against, which are the ones
    /// it is built from.
    pub library: Library,
    /// Indexed by [`DomainId`].
    pub domains: Vec<DomainInfo>,
    pub sources: Vec<Source>,
    pub params: Vec<Param>,
    /// Every signal the spec computes. A node's inputs come before it.
    pub nodes: Vec<Node>,
    pub outputs: Vec<Output>,
}

/// What is known about a rate domain before any data arrives.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainInfo {
    /// Samples per second.
    pub rate: f64,
    pub origin: Origin,
}

/// How a domain came to be.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// A sensor stream, such as `/0/field`.
    Stream(String),
    Decimated {
        from: DomainId,
        factor: i64,
    },
    Resampled {
        from: DomainId,
    },
    Aligned {
        arms: Vec<String>,
    },
    /// Not time at all. These are the bins of a spectrum of `from`,
    /// `resolution` hertz apart, the first at `resolution`. The domain's
    /// rate is bins per hertz.
    Spectrum {
        from: DomainId,
        bins: usize,
        resolution: f64,
    },
}

/// `/route/stream` or `/route/stream/column`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPath {
    pub route: Vec<u8>,
    pub stream: String,
    pub column: Option<String>,
}

impl StreamPath {
    /// The path of the stream, without the column.
    pub fn stream_path(&self) -> String {
        let mut path = String::new();
        for hop in &self.route {
            path.push_str(&format!("/{hop}"));
        }
        format!("{path}/{}", self.stream)
    }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.stream_path())?;
        match &self.column {
            Some(column) => write!(f, "/{column}"),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub name: String,
    pub path: StreamPath,
    /// The node that carries this source's signal.
    pub node: usize,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub name: String,
    pub unit: Unit,
    pub value: f64,
    pub range: Option<(f64, f64)>,
    pub span: Span,
    /// The literal to splice when the value is changed.
    pub value_span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub op: Op,
    pub inputs: Vec<usize>,
    pub ty: SignalType,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Source {
        source: usize,
    },
    /// The channel or group called `name`.
    Select {
        name: String,
    },
    Call {
        function: &'static str,
        args: Vec<CallArg>,
        /// What each arm of an `align` is called.
        arms: Vec<String>,
    },
    Add,
    Subtract,
    Scale {
        factor: Scalar,
        divide: bool,
    },
    Negate,
    /// States the unit of channels that had none.
    Declare {
        unit: Unit,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallArg {
    pub name: &'static str,
    /// The argument as written.
    pub text: String,
    pub value: ArgValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArgValue {
    /// A number that may follow a param, in hertz or seconds if it has a unit.
    Scalar(Scalar),
    Number(f64),
    Integer(i64),
    Choice(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scalar {
    pub expr: ScalarExpr,
    pub unit: Unit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    Number(f64),
    /// An index into [`Program::params`].
    Param(usize),
    Negate(Box<ScalarExpr>),
    Binary(BinaryOp, Box<ScalarExpr>, Box<ScalarExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Output {
    pub name: String,
    pub node: usize,
    pub span: Span,
}

impl Program {
    pub fn domain(&self, id: DomainId) -> &DomainInfo {
        &self.domains[id.0 as usize]
    }

    /// The sample rate of a node's signal.
    pub fn rate_of(&self, node: usize) -> f64 {
        self.domain(self.nodes[node].ty.domain).rate
    }

    /// A node's signal in a few words, such as `500 Hz · 6 ch · nT`. The
    /// unit is given when every channel has the same one.
    pub fn signal_of(&self, node: usize) -> String {
        let ty = &self.nodes[node].ty;
        let mut text = format!("{} · {} ch", self.sampling_of(node), ty.channels.len());
        let unit = ty.channels.get(0).map(|channel| &channel.unit);
        if let Some(unit) = unit.filter(|unit| !unit.is_none()) {
            if ty.channels.iter().all(|channel| channel.unit == *unit) {
                text.push_str(&format!(" · {unit}"));
            }
        }
        text
    }

    /// Where a domain's samples come from, in words.
    pub fn describe(&self, id: DomainId) -> String {
        match &self.domain(id).origin {
            Origin::Stream(path) => path.clone(),
            Origin::Decimated { from, factor } => {
                format!("{}, decimated by {factor}", self.describe(*from))
            }
            Origin::Resampled { from } => format!("{}, resampled", self.describe(*from)),
            Origin::Aligned { arms } => format!("align({})", arms.join(", ")),
            Origin::Spectrum { from, .. } => format!("the spectrum of {}", self.describe(*from)),
        }
    }

    /// How finely a node's signal is sampled, in words. That is `500 Hz`,
    /// or for a spectrum its bins, `255 bins of 1.953125 Hz`.
    pub fn sampling_of(&self, node: usize) -> String {
        match &self.domain(self.nodes[node].ty.domain).origin {
            Origin::Spectrum {
                bins, resolution, ..
            } => format!("{bins} bins of {resolution} Hz"),
            _ => format_rate(self.rate_of(node)),
        }
    }

    /// Whether a node's signal is a spectrum, over frequency, and not a
    /// signal in time.
    pub fn is_spectrum(&self, node: usize) -> bool {
        matches!(
            self.domain(self.nodes[node].ty.domain).origin,
            Origin::Spectrum { .. }
        )
    }

    /// The value of a scalar with every param at its declared value.
    pub fn value(&self, expr: &ScalarExpr) -> f64 {
        self.evaluate(expr, &self.declared())
    }

    /// Each param's declared value, in order.
    pub fn declared(&self) -> Vec<f64> {
        self.params.iter().map(|param| param.value).collect()
    }

    /// The value of a scalar when the params have the values given, one per
    /// param in order.
    pub fn evaluate(&self, expr: &ScalarExpr, values: &[f64]) -> f64 {
        match expr {
            ScalarExpr::Number(value) => *value,
            ScalarExpr::Param(index) => values[*index],
            ScalarExpr::Negate(operand) => -self.evaluate(operand, values),
            ScalarExpr::Binary(op, left, right) => {
                let (left, right) = (self.evaluate(left, values), self.evaluate(right, values));
                match op {
                    BinaryOp::Add => left + right,
                    BinaryOp::Sub => left - right,
                    BinaryOp::Mul => left * right,
                    BinaryOp::Div => left / right,
                }
            }
        }
    }

    /// The least and greatest value a scalar can take as its params move
    /// through their ranges.
    pub fn bounds(&self, expr: &ScalarExpr) -> (f64, f64) {
        let spread = |values: [f64; 4]| {
            let low = values.iter().copied().fold(f64::INFINITY, f64::min);
            let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            (low, high)
        };
        match expr {
            ScalarExpr::Number(value) => (*value, *value),
            ScalarExpr::Param(index) => {
                let param = &self.params[*index];
                param.range.unwrap_or((param.value, param.value))
            }
            ScalarExpr::Negate(operand) => {
                let (low, high) = self.bounds(operand);
                (-high, -low)
            }
            ScalarExpr::Binary(op, left, right) => {
                let (a, b) = self.bounds(left);
                let (c, d) = self.bounds(right);
                match op {
                    BinaryOp::Add => (a + c, b + d),
                    BinaryOp::Sub => (a - d, b - c),
                    BinaryOp::Mul => spread([a * c, a * d, b * c, b * d]),
                    BinaryOp::Div if c <= 0.0 && d >= 0.0 => (f64::NEG_INFINITY, f64::INFINITY),
                    BinaryOp::Div => spread([a / c, a / d, b / c, b / d]),
                }
            }
        }
    }
}

/// Writes a rate the way people say it, as `500 Hz` or `1 kHz`.
pub fn format_rate(rate: f64) -> String {
    if rate >= 1e6 {
        format!("{} MHz", rate / 1e6)
    } else if rate >= 1e3 {
        format!("{} kHz", rate / 1e3)
    } else {
        format!("{rate} Hz")
    }
}

/// Checks a spec. The program is incomplete when there are errors.
pub fn check<'a>(
    file: &'a ast::File,
    source: &'a str,
    library: &'a Library,
) -> (Program, Vec<Diagnostic>) {
    let mut checker = Checker {
        source,
        library,
        program: Program {
            library: library.clone(),
            ..Program::default()
        },
        diagnostics: Vec::new(),
        scope: HashMap::new(),
        declared: HashMap::new(),
        forward: HashSet::new(),
        streams: HashMap::new(),
        derived: HashMap::new(),
        item: Span::at(0),
        functions: HashMap::new(),
        later_functions: HashMap::new(),
        expanding: Vec::new(),
    };
    for item in &file.items {
        if let Some(name) = item.name() {
            checker
                .declared
                .entry(name.text.clone())
                .or_insert(name.span);
            if matches!(item, ast::Item::Fn(_)) {
                checker
                    .later_functions
                    .entry(name.text.clone())
                    .or_insert(name.span);
            }
        }
    }
    for item in &file.items {
        checker.item = item.span();
        match item {
            ast::Item::Source(item) => checker.source(item),
            ast::Item::Param(item) => checker.param(item),
            ast::Item::Let(item) => {
                let value = checker.value(&item.value, item.broken);
                if let Some(name) = &item.name {
                    checker.define(name, Kind::Let, value);
                }
            }
            ast::Item::Out(item) => checker.output(item),
            ast::Item::Fn(item) => checker.function(item),
        }
    }
    checker.unused();
    checker.diagnostics.sort_by_key(|d| d.span.start);
    (checker.program, checker.diagnostics)
}

#[derive(Clone)]
enum Value {
    /// A node of the program.
    Signal(usize),
    Scalar(Scalar),
    /// Something that already failed to check. Using it reports nothing.
    Poison,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Source,
    Param,
    Let,
    Out,
}

#[derive(Clone)]
struct Binding {
    value: Value,
    span: Span,
    kind: Kind,
    used: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Derived {
    Decimate(i64),
    /// The new rate, as bits, so that it can be a key.
    Resample(u64),
    /// The spectrum of a window of this many samples.
    Spectrum(usize),
}

/// One signal handed to a call. It is the expression, and what `align`
/// would call it.
type Arm<'a> = (&'a Expr, Option<String>);

struct Checker<'a> {
    source: &'a str,
    /// The functions the spec may call.
    library: &'a Library,
    program: Program,
    diagnostics: Vec<Diagnostic>,
    scope: HashMap<String, Binding>,
    /// Where each name is first defined anywhere in the file.
    declared: HashMap<String, Span>,
    /// Names used above their definition, which is an error but still a use.
    forward: HashSet<String>,
    /// The domain of each stream a source names, and where it was given.
    streams: HashMap<(Vec<u8>, String), (DomainId, Span)>,
    derived: HashMap<(DomainId, Derived), DomainId>,
    /// The definition being checked.
    item: Span,
    /// The functions the spec has defined so far.
    functions: HashMap<String, Defined<'a>>,
    /// Where each function the spec defines anywhere is named, for a call
    /// that comes before the definition.
    later_functions: HashMap<String, Span>,
    /// The spec's functions being expanded, outermost first.
    expanding: Vec<String>,
}

/// A function the spec defines, which is an expression with names left open.
#[derive(Clone)]
struct Defined<'a> {
    name: Span,
    params: &'a [ast::Ident],
    body: &'a Expr,
    broken: bool,
    /// The names there were when it was defined. Its body sees these and
    /// what a call gives it, and nothing defined since.
    scope: Rc<HashMap<String, Binding>>,
    used: bool,
}

impl<'a> Checker<'a> {
    fn report(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    fn push(&mut self, op: Op, inputs: Vec<usize>, ty: SignalType, span: Span) -> usize {
        self.program.nodes.push(Node {
            op,
            inputs,
            ty,
            span,
        });
        self.program.nodes.len() - 1
    }

    fn ty(&self, node: usize) -> SignalType {
        self.program.nodes[node].ty.clone()
    }

    fn new_domain(&mut self, rate: f64, origin: Origin) -> DomainId {
        self.program.domains.push(DomainInfo { rate, origin });
        DomainId(self.program.domains.len() as u32 - 1)
    }

    /// A rate and where it comes from, as an arm of a mismatch is labelled.
    fn rate_label(&self, node: usize) -> String {
        format!(
            "{} (from {})",
            self.program.sampling_of(node),
            self.program.describe(self.program.nodes[node].ty.domain)
        )
    }

    // Names.

    /// Binds `name`, unless it is taken. Returns whether it was bound.
    fn define(&mut self, name: &ast::Ident, kind: Kind, value: Value) -> bool {
        let taken = self
            .scope
            .get(&name.text)
            .map(|binding| binding.span)
            .or_else(|| self.functions.get(&name.text).map(|function| function.name));
        if let Some(first) = taken {
            self.report(
                Diagnostic::error(
                    code::DEFINED_TWICE,
                    name.span,
                    format!("`{}` is defined twice", name.text),
                )
                .label("defined again here")
                .secondary(first, "first defined here")
                .note("sources, params, lets, outs and functions share one set of names"),
            );
            return false;
        }
        self.scope.insert(
            name.text.clone(),
            Binding {
                value,
                span: name.span,
                kind,
                used: kind == Kind::Out || self.forward.remove(&name.text),
            },
        );
        true
    }

    fn lookup(&mut self, name: &ast::Ident) -> Value {
        if let Some(binding) = self.scope.get_mut(&name.text) {
            binding.used = true;
            return binding.value.clone();
        }
        // A unit by itself is one of that unit, so that `10 m / s` reads as
        // it would on paper. A definition of the same name comes first.
        if !self.declared.contains_key(&name.text) && units::is_known(&name.text) {
            return Value::Scalar(Scalar {
                expr: ScalarExpr::Number(1.0),
                unit: Unit::new(name.text.clone()),
            });
        }
        let diagnostic = if let Some(&defined) = self.declared.get(&name.text) {
            self.forward.insert(name.text.clone());
            if self.item.start <= defined.start && defined.end <= self.item.end {
                Diagnostic::error(
                    code::USED_BEFORE_DEFINITION,
                    name.span,
                    format!("`{}` is used in its own definition", name.text),
                )
                .label("used here")
                .note("a graph has no feedback paths")
            } else {
                Diagnostic::error(
                    code::USED_BEFORE_DEFINITION,
                    name.span,
                    format!("`{}` is used before it is defined", name.text),
                )
                .label("used here")
                .secondary(defined, "defined here")
                .help("move the definition above its first use")
            }
        } else {
            let diagnostic = Diagnostic::error(
                code::UNKNOWN_NAME,
                name.span,
                format!("cannot find `{}`", name.text),
            )
            .label("not a source, param, let or out");
            let mut names: Vec<&str> = self.scope.keys().map(String::as_str).collect();
            names.sort_unstable();
            if let Some(builtin) = self.library.find(&name.text) {
                diagnostic.help(format!(
                    "`{}` is a function, written `{}`",
                    builtin.name, builtin.usage
                ))
            } else if let Some(near) = builtins::closest(&name.text, names) {
                diagnostic.help(format!("did you mean `{near}`?"))
            } else {
                diagnostic
            }
        };
        self.report(diagnostic);
        Value::Poison
    }

    /// Marks the names in `expr` as used, without checking it.
    fn mark_used(&mut self, expr: &Expr) {
        match expr {
            Expr::Name(name) => {
                if let Some(binding) = self.scope.get_mut(&name.text) {
                    binding.used = true;
                }
            }
            Expr::Call(call) => self.mark_call_used(call),
            Expr::Pipe { input, call, .. } => {
                self.mark_used(input);
                self.mark_call_used(call);
            }
            Expr::Binary { left, right, .. } => {
                self.mark_used(left);
                self.mark_used(right);
            }
            Expr::Negate { operand: inner, .. }
            | Expr::Select { base: inner, .. }
            | Expr::Paren { inner, .. }
            | Expr::In { value: inner, .. } => self.mark_used(inner),
            Expr::Number(_) | Expr::Path(_) | Expr::Missing(_) => {}
        }
    }

    fn mark_call_used(&mut self, call: &ast::Call) {
        for arg in &call.args {
            self.mark_used(&arg.value);
        }
    }

    /// Takes a function's definition. Its body is checked where it is
    /// called, with what the call gives it, and not here. Until then there
    /// is nothing to check it against.
    fn function(&mut self, item: &'a ast::Function) {
        let Some(name) = &item.name else { return };
        if let Some(public) = self.library.find(&name.text) {
            self.report(
                Diagnostic::error(
                    code::DEFINED_TWICE,
                    name.span,
                    format!("there is already a function `{}`", name.text),
                )
                .label("this name is taken")
                .help(format!("it is written `{}`", public.usage)),
            );
            return;
        }
        let taken = self
            .scope
            .get(&name.text)
            .map(|binding| binding.span)
            .or_else(|| self.functions.get(&name.text).map(|function| function.name));
        if let Some(first) = taken {
            self.report(
                Diagnostic::error(
                    code::DEFINED_TWICE,
                    name.span,
                    format!("`{}` is defined twice", name.text),
                )
                .label("defined again here")
                .secondary(first, "first defined here")
                .note("sources, params, lets, outs and functions share one set of names"),
            );
            return;
        }
        for (index, param) in item.params.iter().enumerate() {
            if let Some(first) = item.params[..index].iter().find(|p| p.text == param.text) {
                self.report(
                    Diagnostic::error(
                        code::DEFINED_TWICE,
                        param.span,
                        format!(
                            "`{}` is given two things called `{}`",
                            name.text, param.text
                        ),
                    )
                    .label("named again here")
                    .secondary(first.span, "first named here"),
                );
                return;
            }
        }
        self.functions.insert(
            name.text.clone(),
            Defined {
                name: name.span,
                params: &item.params,
                body: &item.body,
                broken: item.broken,
                scope: Rc::new(self.scope.clone()),
                used: self.forward.remove(&name.text),
            },
        );
    }

    /// A call of a function the spec defines. It is the function's body,
    /// with the names it left open bound to what the call gives. Nothing of
    /// the function is left in the program but the nodes its body makes.
    fn expand(
        &mut self,
        name: &str,
        function: Defined<'a>,
        call: &ast::Call,
        piped: Option<&Expr>,
    ) -> Value {
        if let Some(defined) = self.functions.get_mut(name) {
            defined.used = true;
        }
        let written = || {
            let params: Vec<&str> = function.params.iter().map(|p| p.text.as_str()).collect();
            format!("it is written `{name}({})`", params.join(", "))
        };

        // What it is given: what is piped in first, then the rest in the
        // order written, or by name.
        let mut given: Vec<Option<Value>> = vec![None; function.params.len()];
        let mut ok = true;
        let mut place = |checker: &mut Self, expr: &Expr, named: Option<&ast::Ident>| {
            let slot = match named {
                Some(named) => function.params.iter().position(|p| p.text == named.text),
                None => given.iter().position(Option::is_none),
            };
            let value = checker.expr(expr);
            match (slot, named) {
                (Some(slot), _) if given[slot].is_none() => given[slot] = Some(value),
                (Some(slot), Some(named)) => {
                    ok = false;
                    checker.report(
                        Diagnostic::error(
                            code::ARGUMENTS,
                            named.span,
                            format!("`{}` is given twice", function.params[slot].text),
                        )
                        .label("given again here")
                        .help(written()),
                    );
                }
                (None, Some(named)) => {
                    ok = false;
                    checker.report(
                        Diagnostic::error(
                            code::ARGUMENTS,
                            named.span,
                            format!("`{name}` is given nothing called `{}`", named.text),
                        )
                        .label("not one of its names")
                        .help(written()),
                    );
                }
                (_, None) => {
                    ok = false;
                    checker.report(
                        Diagnostic::error(
                            code::ARGUMENTS,
                            expr.span(),
                            format!("`{name}` takes no more"),
                        )
                        .label("one too many")
                        .help(written()),
                    );
                }
            }
        };
        if let Some(input) = piped {
            place(self, input, None);
        }
        for arg in &call.args {
            place(self, &arg.value, arg.name.as_ref());
        }
        let missing: Vec<&str> = function
            .params
            .iter()
            .zip(&given)
            .filter(|(_, value)| value.is_none())
            .map(|(param, _)| param.text.as_str())
            .collect();
        if ok && !missing.is_empty() {
            ok = false;
            let names: Vec<String> = missing.iter().map(|m| format!("`{m}`")).collect();
            self.report(
                Diagnostic::error(
                    code::ARGUMENTS,
                    call.span,
                    format!("`{name}` needs {}", names.join(" and ")),
                )
                .label("not given")
                .help(written()),
            );
        }
        if !ok || function.broken {
            return Value::Poison;
        }
        if self.expanding.iter().any(|outer| outer == name) {
            self.report(
                Diagnostic::error(
                    code::USED_BEFORE_DEFINITION,
                    call.name.span,
                    format!("`{name}` is used in its own definition"),
                )
                .label("used here")
                .note("a graph has no feedback paths"),
            );
            return Value::Poison;
        }

        // The body sees the names of its definition and what it was given.
        let mut scope = (*function.scope).clone();
        for (param, value) in function.params.iter().zip(given) {
            scope.insert(
                param.text.clone(),
                Binding {
                    value: value.unwrap_or(Value::Poison),
                    span: param.span,
                    kind: Kind::Let,
                    used: true,
                },
            );
        }
        let outer = std::mem::replace(&mut self.scope, scope);
        let before = self.diagnostics.len();
        self.expanding.push(name.to_owned());
        let value = self.expr(function.body);
        self.expanding.pop();
        let inner = std::mem::replace(&mut self.scope, outer);
        // What the body used is used.
        for (used, binding) in inner {
            if !binding.used {
                continue;
            }
            if let Some(live) = self.scope.get_mut(&used) {
                if live.span == binding.span {
                    live.used = true;
                }
            }
        }
        // A name the body reached for that was defined only after it is an
        // error there, and still a use of that name.
        let reached: Vec<String> = self
            .forward
            .iter()
            .filter(|name| self.scope.contains_key(*name))
            .cloned()
            .collect();
        for name in reached {
            self.forward.remove(&name);
            if let Some(live) = self.scope.get_mut(&name) {
                live.used = true;
            }
        }
        // Whatever went wrong in the body went wrong for this call of it.
        for diagnostic in &mut self.diagnostics[before..] {
            diagnostic
                .labels
                .push((call.name.span, format!("in this call of `{name}`")));
        }
        value
    }

    fn unused(&mut self) {
        let mut uncalled: Vec<(Span, String)> = self
            .functions
            .iter()
            .filter(|(_, function)| !function.used && !function.broken)
            .map(|(name, function)| (function.name, name.clone()))
            .collect();
        uncalled.sort_by_key(|(span, _)| span.start);
        for (span, name) in uncalled {
            self.report(
                Diagnostic::warning(code::UNUSED, span, format!("`{name}` is never used"))
                    .label("nothing calls this function"),
            );
        }
        let mut unused: Vec<(Span, String, Kind)> = self
            .scope
            .iter()
            .filter(|(_, b)| !b.used && !matches!(b.value, Value::Poison))
            .map(|(name, b)| (b.span, name.clone(), b.kind))
            .collect();
        unused.sort_by_key(|(span, ..)| span.start);
        for (span, name, kind) in unused {
            let kind = match kind {
                Kind::Source => "source",
                Kind::Param => "param",
                Kind::Let | Kind::Out => "let",
            };
            self.report(
                Diagnostic::warning(code::UNUSED, span, format!("`{name}` is never used"))
                    .label(format!("nothing refers to this {kind}")),
            );
        }
    }

    // Definitions.

    fn source(&mut self, item: &ast::Source) {
        let Some(name) = &item.name else { return };
        let value = self
            .source_node(item, name)
            .map_or(Value::Poison, Value::Signal);
        self.define(name, Kind::Source, value);
    }

    fn source_node(&mut self, item: &ast::Source, name: &ast::Ident) -> Option<usize> {
        let written = item.path.as_ref()?;
        let path = self.path(written)?;

        let mut rate = None;
        let mut names: Option<(Vec<String>, Span)> = None;
        let mut unit = Unit::none();
        let mut ok = true;
        for attr in &item.attrs {
            let Some(attr_name) = &attr.name else {
                ok = false;
                continue;
            };
            let malformed = |example: &str| {
                Diagnostic::error(
                    code::ATTRIBUTE,
                    attr.span,
                    format!("`@{}` is written `{example}`", attr_name.text),
                )
            };
            match attr_name.text.as_str() {
                "rate" => match attr.args.as_slice() {
                    [ast::Arg {
                        name: None,
                        value: Expr::Number(number),
                        ..
                    }] if number.value > 0.0 => match in_hertz(number) {
                        Ok(hertz) => rate = Some(hertz),
                        Err(error) => {
                            self.report(malformed("@rate(1000)").label(error.to_string()));
                            ok = false;
                        }
                    },
                    _ => {
                        self.report(malformed("@rate(1000)"));
                        ok = false;
                    }
                },
                "channels" => {
                    let listed: Option<Vec<String>> = attr
                        .args
                        .iter()
                        .map(|arg| {
                            let text = arg.value.span().text(self.source);
                            let named = arg.name.is_none()
                                && !text.is_empty()
                                && text
                                    .split('.')
                                    .all(|part| !part.is_empty() && part.chars().all(is_name_char));
                            named.then(|| text.to_owned())
                        })
                        .collect();
                    match listed {
                        Some(listed) if !listed.is_empty() => names = Some((listed, attr.span)),
                        _ => {
                            self.report(malformed("@channels(x, y, z)"));
                            ok = false;
                        }
                    }
                }
                "unit" => {
                    let written = match attr.args.as_slice() {
                        [arg] if arg.name.is_none() => {
                            UnitExpr::parse(arg.value.span().text(self.source))
                        }
                        _ => None,
                    };
                    match written {
                        Some(written) if !written.is_none() => unit = written.label(),
                        _ => {
                            self.report(malformed("@unit(nT)"));
                            ok = false;
                        }
                    }
                }
                // Where the node sits on the canvas. Nothing to check.
                "pos" => {}
                other => {
                    self.report(
                        Diagnostic::error(
                            code::ATTRIBUTE,
                            attr_name.span,
                            format!("unknown attribute `@{other}`"),
                        )
                        .help("a source takes `@rate`, `@channels`, `@unit` and `@pos`"),
                    );
                    ok = false;
                }
            }
        }
        if !ok {
            return None;
        }
        if let (Some(_), Some((_, span))) = (&path.column, &names) {
            self.report(
                Diagnostic::error(
                    code::ATTRIBUTE,
                    *span,
                    "`@channels` does not apply to a single column",
                )
                .secondary(written.span, "this path names one column"),
            );
            return None;
        }

        let needs_channels = path.column.is_none() && names.is_none();
        if rate.is_none() || needs_channels {
            let (what, verb, them) = match (rate.is_none(), needs_channels) {
                (true, true) => ("sample rate and channels", "are", "them"),
                (true, false) => ("sample rate", "is", "it"),
                _ => ("channels", "are", "them"),
            };
            let mut example = format!("source {} = {}", name.text, written.text);
            for attr in &item.attrs {
                example.push(' ');
                example.push_str(attr.span.text(self.source));
            }
            if rate.is_none() {
                example.push_str(" @rate(1000)");
            }
            if needs_channels {
                example.push_str(" @channels(x, y, z)");
            }
            self.report(
                Diagnostic::error(
                    code::SOURCE_TYPE_UNKNOWN,
                    written.span,
                    format!("the {what} of `{}` {verb} not known", name.text),
                )
                .label("there is no device here to ask")
                .help(format!(
                    "state {them} on the source, for example:\n  {example}"
                ))
                .note(
                    "a spec checked without a device takes rates and channel names\n\
                     from its attributes",
                ),
            );
            return None;
        }
        let rate = rate?;

        let stream = (path.route.clone(), path.stream.clone());
        let domain = match self.streams.get(&stream) {
            Some(&(domain, first)) => {
                let known = self.program.domain(domain).rate;
                if known != rate {
                    self.report(
                        Diagnostic::error(
                            code::ATTRIBUTE,
                            written.span,
                            format!("`{}` is given two rates", path.stream_path()),
                        )
                        .label(format!("{} here", format_rate(rate)))
                        .secondary(first, format!("{} here", format_rate(known))),
                    );
                    return None;
                }
                domain
            }
            None => {
                let domain = self.new_domain(rate, Origin::Stream(path.stream_path()));
                self.streams.insert(stream, (domain, written.span));
                domain
            }
        };

        let (listed, listed_span) = match (&path.column, names) {
            (Some(column), _) => (vec![column.clone()], written.span),
            (None, Some(names)) => names,
            (None, None) => return None,
        };
        let channels = listed
            .into_iter()
            .map(|name| Channel {
                name,
                unit: unit.clone(),
            })
            .collect();
        let channels = match ChannelSet::new(channels) {
            Ok(channels) => channels,
            Err(error) => {
                self.report(Diagnostic::error(
                    code::ATTRIBUTE,
                    listed_span,
                    error.to_string(),
                ));
                return None;
            }
        };
        let node = self.push(
            Op::Source {
                source: self.program.sources.len(),
            },
            Vec::new(),
            SignalType { domain, channels },
            item.span,
        );
        self.program.sources.push(Source {
            name: name.text.clone(),
            path,
            node,
            span: item.span,
        });
        Some(node)
    }

    fn path(&mut self, written: &ast::Path) -> Option<StreamPath> {
        let malformed = |message: String| {
            Diagnostic::error(code::STREAM_PATH, written.span, message).help(
                "a stream path is `/route/stream` or `/route/stream/column`,\n\
                 such as `/0/field` or `/0/field/x`",
            )
        };
        let parts: Vec<&str> = written.text.split('/').skip(1).collect();
        let hops = parts
            .iter()
            .take_while(|part| part.chars().all(|c| c.is_ascii_digit()))
            .count();
        let mut route = Vec::new();
        for hop in &parts[..hops] {
            match hop.parse::<u8>() {
                Ok(hop) if route.len() < 8 => route.push(hop),
                Ok(_) => {
                    self.report(malformed("a route has at most eight hops".to_owned()));
                    return None;
                }
                Err(_) => {
                    self.report(malformed(format!(
                        "`{hop}` is not a route hop, which is a number from 0 to 255"
                    )));
                    return None;
                }
            }
        }
        match &parts[hops..] {
            [stream] => Some(StreamPath {
                route,
                stream: (*stream).to_owned(),
                column: None,
            }),
            [stream, column] => Some(StreamPath {
                route,
                stream: (*stream).to_owned(),
                column: Some((*column).to_owned()),
            }),
            [] => {
                self.report(malformed(format!(
                    "`{}` names a device, not one of its streams",
                    written.text
                )));
                None
            }
            _ => {
                self.report(malformed(format!(
                    "`{}` has parts after its column",
                    written.text
                )));
                None
            }
        }
    }

    fn param(&mut self, item: &ast::Param) {
        let Some(name) = &item.name else { return };
        let value = match self.param_index(item, name) {
            Some(index) => Value::Scalar(Scalar {
                expr: ScalarExpr::Param(index),
                unit: self.program.params[index].unit.clone(),
            }),
            None => Value::Poison,
        };
        self.define(name, Kind::Param, value);
    }

    fn param_index(&mut self, item: &ast::Param, name: &ast::Ident) -> Option<usize> {
        let value = item.value.as_ref()?;
        let declared = match &item.unit {
            Some(written) => Some((self.unit_from(written)?, written)),
            None => None,
        };
        let given = value.unit.as_deref().map(canonical);
        let unit = match (declared, given) {
            (Some((declared, written)), Some(given)) if declared != given => {
                self.report(
                    Diagnostic::error(
                        code::PARAM,
                        value.span,
                        format!(
                            "`{}` is declared in {declared} and given in {given}",
                            name.text
                        ),
                    )
                    .label(format!("in {given}"))
                    .secondary(written.span, format!("declared in {declared}")),
                );
                return None;
            }
            (Some((declared, _)), _) => declared,
            (None, Some(given)) => given,
            (None, None) => Unit::none(),
        };
        let range = match &item.range {
            None => None,
            Some(range) => {
                let (low, high) = (range.low.as_ref()?, range.high.as_ref()?);
                for end in [low, high] {
                    if let Some(given) = end.unit.as_ref().filter(|g| *g != unit.symbol()) {
                        self.report(
                            Diagnostic::error(
                                code::PARAM,
                                end.span,
                                format!("the range of `{}` is in another unit", name.text),
                            )
                            .label(format!("in {given}, and `{}` is in {unit}", name.text)),
                        );
                        return None;
                    }
                }
                if low.value >= high.value {
                    self.report(
                        Diagnostic::error(
                            code::PARAM,
                            range.span,
                            format!("the range of `{}` is empty", name.text),
                        )
                        .label(format!("{} is not below {}", low.value, high.value)),
                    );
                    return None;
                }
                if value.value < low.value || value.value > high.value {
                    self.report(
                        Diagnostic::error(
                            code::PARAM,
                            value.span,
                            format!("`{}` starts outside its range", name.text),
                        )
                        .label(format!("{} is not within", value.value))
                        .secondary(range.span, "this range"),
                    );
                    return None;
                }
                Some((low.value, high.value))
            }
        };
        self.program.params.push(Param {
            name: name.text.clone(),
            unit,
            value: value.value,
            range,
            span: item.span,
            value_span: value.digits,
        });
        Some(self.program.params.len() - 1)
    }

    fn output(&mut self, item: &ast::Out) {
        let value = self.value(&item.value, item.broken);
        let Some(name) = &item.name else { return };
        let node = match value {
            Value::Signal(node) => self.declare(node, item, name),
            Value::Scalar(_) => {
                self.report(
                    Diagnostic::error(
                        code::WRONG_KIND,
                        item.value.span(),
                        format!("`{}` is a number, and an output is a signal", name.text),
                    )
                    .label("a number")
                    .note("an output is published as a stream of samples"),
                );
                None
            }
            Value::Poison => None,
        };
        let value = node.map_or(Value::Poison, Value::Signal);
        if let (true, Some(node)) = (self.define(name, Kind::Out, value), node) {
            self.program.outputs.push(Output {
                name: name.text.clone(),
                node,
                span: item.span,
            });
        }
    }

    /// Applies an output's declared unit. Channels without a unit take it,
    /// and channels in another unit are an error. A unit is never converted
    /// unasked.
    fn declare(&mut self, node: usize, item: &ast::Out, name: &ast::Ident) -> Option<usize> {
        let Some(written) = &item.unit else {
            return Some(node);
        };
        let declared = self.unit_from(written)?;
        let ty = self.ty(node);
        if let Some(other) = ty
            .channels
            .iter()
            .find(|c| !c.unit.is_none() && c.unit != declared)
        {
            let diagnostic = Diagnostic::error(
                code::UNITS,
                written.span,
                format!(
                    "`{}` is declared in {declared}, and its value is in {}",
                    name.text, other.unit
                ),
            )
            .label("declared here")
            .secondary(
                item.value.span(),
                format!("`{}` is in {}", other.name, other.unit),
            );
            let convertible = UnitExpr::of(&other.unit)
                .convert(&UnitExpr::of(&declared))
                .is_ok();
            self.report(if convertible {
                diagnostic.help(format!("convert it: `… in {declared}`"))
            } else {
                diagnostic
            });
            return None;
        }
        if ty.channels.iter().all(|c| c.unit == declared) {
            return Some(node);
        }
        let channels = relabel(&ty.channels, |channel| Channel {
            name: channel.name.clone(),
            unit: declared.clone(),
        });
        Some(self.push(
            Op::Declare { unit: declared },
            vec![node],
            SignalType {
                domain: ty.domain,
                channels,
            },
            item.span,
        ))
    }

    /// Reads a unit written after a colon or an `in`.
    fn unit_from(&mut self, written: &ast::Ident) -> Option<Unit> {
        match UnitExpr::parse(&written.text) {
            Some(unit) if !unit.is_none() => Some(unit.label()),
            _ => {
                self.report(
                    Diagnostic::error(
                        code::UNITS,
                        written.span,
                        format!("`{}` does not read as a unit", written.text),
                    )
                    .help("a unit is written like `nT`, `nT/V` or `m/s^2`"),
                );
                None
            }
        }
    }

    /// `value in unit`, which is the same quantity counted in another unit.
    /// A value with no unit takes the unit as it stands.
    fn convert(&mut self, span: Span, value: &Expr, written: &ast::Ident) -> Value {
        let converted = self.expr(value);
        let Some(target) = self.unit_from(written) else {
            return Value::Poison;
        };
        let to = UnitExpr::of(&target);
        let refuse = |checker: &mut Self, error: units::Inconvertible, what: String| {
            checker.report(
                Diagnostic::error(code::UNITS, written.span, error.to_string())
                    .label(format!("asked for in {target}"))
                    .secondary(value.span(), what),
            );
            Value::Poison
        };
        match converted {
            Value::Poison => Value::Poison,
            Value::Scalar(scalar) if scalar.unit.is_none() => Value::Scalar(Scalar {
                expr: scalar.expr,
                unit: target,
            }),
            Value::Scalar(scalar) => match UnitExpr::of(&scalar.unit).convert(&to) {
                Ok(factor) => Value::Scalar(Scalar {
                    expr: scaled(scalar.expr, factor),
                    unit: target,
                }),
                Err(error) => {
                    let what = format!("in {}", scalar.unit);
                    refuse(self, error, what)
                }
            },
            Value::Signal(node) => {
                let ty = self.ty(node);
                let mut factor = 1.0;
                for (index, channel) in ty.channels.iter().enumerate() {
                    let needed = if channel.unit.is_none() {
                        Ok(1.0)
                    } else {
                        UnitExpr::of(&channel.unit).convert(&to)
                    };
                    match needed {
                        Ok(needed) if index == 0 || needed == factor => factor = needed,
                        Ok(_) => {
                            self.report(
                                Diagnostic::error(
                                    code::UNITS,
                                    value.span(),
                                    "the channels of this signal are in different units",
                                )
                                .label("one factor does not convert them all")
                                .help("select the channels in one unit first"),
                            );
                            return Value::Poison;
                        }
                        Err(error) => {
                            let what = format!("`{}` is in {}", channel.name, channel.unit);
                            return refuse(self, error, what);
                        }
                    }
                }
                if ty.channels.iter().all(|c| c.unit == target) {
                    return Value::Signal(node);
                }
                let converted = SignalType {
                    domain: ty.domain,
                    channels: relabel(&ty.channels, |channel| Channel {
                        name: channel.name.clone(),
                        unit: target.clone(),
                    }),
                };
                let op = if factor == 1.0 {
                    Op::Declare { unit: target }
                } else {
                    Op::Scale {
                        factor: Scalar {
                            expr: ScalarExpr::Number(factor),
                            unit: Unit::none(),
                        },
                        divide: false,
                    }
                };
                Value::Signal(self.push(op, vec![node], converted, span))
            }
        }
    }

    // Expressions.

    /// The value of a definition. One the parser gave up on is unknown, but
    /// the names it got as far as mentioning are still in use.
    fn value(&mut self, expr: &Expr, broken: bool) -> Value {
        if broken {
            self.mark_used(expr);
            Value::Poison
        } else {
            self.expr(expr)
        }
    }

    fn expr(&mut self, expr: &Expr) -> Value {
        match expr {
            Expr::Missing(_) => Value::Poison,
            Expr::Name(name) => self.lookup(name),
            Expr::Number(number) => Value::Scalar(Scalar {
                expr: ScalarExpr::Number(number.value),
                unit: number.unit.as_deref().map_or_else(Unit::none, canonical),
            }),
            Expr::Path(path) => self.inline_path(path),
            Expr::Paren { inner, .. } => self.expr(inner),
            Expr::In { span, value, unit } => self.convert(*span, value, unit),
            Expr::Negate { span, operand } => match self.expr(operand) {
                Value::Signal(node) => {
                    let ty = self.ty(node);
                    Value::Signal(self.push(Op::Negate, vec![node], ty, *span))
                }
                Value::Scalar(scalar) => Value::Scalar(Scalar {
                    expr: ScalarExpr::Negate(Box::new(scalar.expr)),
                    unit: scalar.unit,
                }),
                Value::Poison => Value::Poison,
            },
            Expr::Select { span, base, name } => match self.expr(base) {
                Value::Signal(node) => self.select(node, base.span(), name, *span),
                Value::Scalar(_) => {
                    self.report(
                        Diagnostic::error(
                            code::WRONG_KIND,
                            base.span(),
                            "a number has no channels to select",
                        )
                        .label("a number")
                        .secondary(name.span, "selects a channel"),
                    );
                    Value::Poison
                }
                Value::Poison => Value::Poison,
            },
            Expr::Binary {
                span,
                op,
                left,
                right,
            } => self.binary(*span, *op, left, right),
            Expr::Call(call) => self.call(call, None, call.span),
            Expr::Pipe { span, input, call } => self.call(call, Some(input), *span),
        }
    }

    /// A stream path used in an expression. It takes its type from a source
    /// that names the same stream.
    fn inline_path(&mut self, written: &ast::Path) -> Value {
        let Some(path) = self.path(written) else {
            return Value::Poison;
        };
        let declared = self
            .program
            .sources
            .iter()
            .find(|s| s.path.route == path.route && s.path.stream == path.stream)
            .map(|s| (s.name.clone(), s.node, s.path.column.clone()));
        let Some((name, node, column)) = declared else {
            self.report(
                Diagnostic::error(
                    code::SOURCE_TYPE_UNKNOWN,
                    written.span,
                    format!("the sample rate and channels of `{}` are not known", written.text),
                )
                .label("no source names this stream")
                .help(format!(
                    "declare it first, for example:\n  source s = {} @rate(1000) @channels(x, y, z)",
                    path.stream_path()
                )),
            );
            return Value::Poison;
        };
        if let Some(binding) = self.scope.get_mut(&name) {
            binding.used = true;
        }
        match (&path.column, &column) {
            (None, None) => Value::Signal(node),
            (Some(wanted), Some(held)) if wanted == held => Value::Signal(node),
            (Some(wanted), None) => {
                let name = ast::Ident {
                    text: wanted.clone(),
                    span: written.span,
                };
                self.select(node, written.span, &name, written.span)
            }
            _ => {
                self.report(
                    Diagnostic::error(
                        code::NO_CHANNEL,
                        written.span,
                        format!("source `{name}` does not carry `{}`", written.text),
                    )
                    .label("not the column that source names"),
                );
                Value::Poison
            }
        }
    }

    fn select(&mut self, node: usize, base: Span, name: &ast::Ident, span: Span) -> Value {
        let ty = self.ty(node);
        let prefix = format!("{}.", name.text);
        let channels: Vec<Channel> = match ty.channels.iter().find(|c| c.name == name.text) {
            Some(channel) => vec![channel.clone()],
            None => ty
                .channels
                .iter()
                .filter_map(|channel| {
                    channel.name.strip_prefix(&prefix).map(|rest| Channel {
                        name: rest.to_owned(),
                        unit: channel.unit.clone(),
                    })
                })
                .collect(),
        };
        if channels.is_empty() {
            let mut heads: Vec<&str> = Vec::new();
            for channel in ty.channels.iter() {
                let head = channel.name.split('.').next().unwrap_or(&channel.name);
                if !heads.contains(&head) {
                    heads.push(head);
                }
            }
            self.report(
                Diagnostic::error(
                    code::NO_CHANNEL,
                    name.span,
                    format!(
                        "`{}` has no channel or group called `{}`",
                        base.text(self.source),
                        name.text
                    ),
                )
                .label("not found")
                .help(format!("it has: {}", heads.join(", "))),
            );
            return Value::Poison;
        }
        let channels = ChannelSet::new(channels).expect("selected names stay unique");
        Value::Signal(self.push(
            Op::Select {
                name: name.text.clone(),
            },
            vec![node],
            SignalType {
                domain: ty.domain,
                channels,
            },
            span,
        ))
    }

    fn binary(&mut self, span: Span, op: BinaryOp, left: &Expr, right: &Expr) -> Value {
        let (a, b) = (self.expr(left), self.expr(right));
        let symbol = op.symbol();
        let refuse = |checker: &mut Self, message: String, help: &str| {
            checker.report(
                Diagnostic::error(code::OPERANDS, span, message)
                    .secondary(left.span(), kind_of(&a))
                    .secondary(right.span(), kind_of(&b))
                    .help(help),
            );
            Value::Poison
        };
        match (&a, &b) {
            (Value::Poison, _) | (_, Value::Poison) => Value::Poison,
            (Value::Signal(l), Value::Signal(r)) => match op {
                BinaryOp::Add | BinaryOp::Sub => self.combine(span, op, (*l, left), (*r, right)),
                BinaryOp::Mul | BinaryOp::Div => refuse(
                    self,
                    format!("`{symbol}` does not combine two signals"),
                    "a signal is scaled by a number or a param; signals add and subtract",
                ),
            },
            (Value::Signal(node), Value::Scalar(factor)) => match op {
                BinaryOp::Mul | BinaryOp::Div => {
                    self.scale(span, *node, factor.clone(), op == BinaryOp::Div)
                }
                BinaryOp::Add | BinaryOp::Sub => refuse(
                    self,
                    format!("`{symbol}` does not combine a signal with a number"),
                    "an offset is not offered; `detrend(mean)` removes one",
                ),
            },
            (Value::Scalar(factor), Value::Signal(node)) => match op {
                BinaryOp::Mul => self.scale(span, *node, factor.clone(), false),
                _ => refuse(
                    self,
                    format!("`{symbol}` does not combine a number with a signal"),
                    "write the signal first, as in `x * 2` or `x / 2`",
                ),
            },
            (Value::Scalar(l), Value::Scalar(r)) => {
                let unit = match op {
                    BinaryOp::Add | BinaryOp::Sub => {
                        if !l.unit.is_none() && !r.unit.is_none() && l.unit != r.unit {
                            self.report(
                                Diagnostic::error(
                                    code::UNITS,
                                    span,
                                    format!("`{symbol}` combines {} with {}", l.unit, r.unit),
                                )
                                .secondary(left.span(), format!("in {}", l.unit))
                                .secondary(right.span(), format!("in {}", r.unit)),
                            );
                            return Value::Poison;
                        }
                        if l.unit.is_none() {
                            r.unit.clone()
                        } else {
                            l.unit.clone()
                        }
                    }
                    BinaryOp::Mul => UnitExpr::of(&l.unit).times(&UnitExpr::of(&r.unit)).label(),
                    BinaryOp::Div => UnitExpr::of(&l.unit).over(&UnitExpr::of(&r.unit)).label(),
                };
                Value::Scalar(Scalar {
                    expr: ScalarExpr::Binary(
                        op,
                        Box::new(l.expr.clone()),
                        Box::new(r.expr.clone()),
                    ),
                    unit,
                })
            }
        }
    }

    /// Adds or subtracts two signals, channel by channel, paired by name.
    fn combine(
        &mut self,
        span: Span,
        op: BinaryOp,
        (left, left_expr): (usize, &Expr),
        (right, right_expr): (usize, &Expr),
    ) -> Value {
        let (a, b) = (self.ty(left), self.ty(right));
        let symbol = op.symbol();
        if a.domain != b.domain {
            let verb = if op == BinaryOp::Add {
                "add"
            } else {
                "subtract"
            };
            self.report(
                Diagnostic::error(
                    code::DOMAINS,
                    right_expr.span(),
                    format!("cannot {verb} signals in different rate domains"),
                )
                .label(self.rate_label(right))
                .secondary(left_expr.span(), self.rate_label(left))
                .note(
                    "signals in different domains share no sample instants, even at\n\
                     the same rate",
                )
                .help("bring them into one domain first, with `align(…)`"),
            );
            return Value::Poison;
        }
        let names = |ty: &SignalType| -> String {
            let names: Vec<&str> = ty.channels.iter().map(|c| c.name.as_str()).collect();
            format!("has {}", names.join(", "))
        };
        let unpaired: Vec<&str> = a
            .channels
            .iter()
            .filter(|c| b.channels.position(&c.name).is_none())
            .chain(
                b.channels
                    .iter()
                    .filter(|c| a.channels.position(&c.name).is_none()),
            )
            .map(|c| c.name.as_str())
            .collect();
        if !unpaired.is_empty() {
            self.report(
                Diagnostic::error(
                    code::UNPAIRED_CHANNELS,
                    span,
                    format!("`{symbol}` pairs channels by name, and these do not pair"),
                )
                .secondary(left_expr.span(), names(&a))
                .secondary(right_expr.span(), names(&b))
                .note(format!("without a partner: {}", unpaired.join(", "))),
            );
            return Value::Poison;
        }
        for channel in a.channels.iter() {
            let partner = b
                .channels
                .iter()
                .find(|c| c.name == channel.name)
                .expect("every channel has a partner");
            if channel.unit != partner.unit {
                self.report(
                    Diagnostic::error(
                        code::UNITS,
                        span,
                        format!(
                            "`{symbol}` combines `{}` in {} with `{}` in {}",
                            channel.name, channel.unit, partner.name, partner.unit
                        ),
                    )
                    .secondary(left_expr.span(), format!("in {}", channel.unit))
                    .secondary(right_expr.span(), format!("in {}", partner.unit)),
                );
                return Value::Poison;
            }
        }
        let op = if op == BinaryOp::Add {
            Op::Add
        } else {
            Op::Subtract
        };
        Value::Signal(self.push(op, vec![left, right], a, span))
    }

    fn scale(&mut self, span: Span, node: usize, factor: Scalar, divide: bool) -> Value {
        let mut ty = self.ty(node);
        if !factor.unit.is_none() {
            let by = UnitExpr::of(&factor.unit);
            ty.channels = relabel(&ty.channels, |channel| {
                let unit = UnitExpr::of(&channel.unit);
                Channel {
                    name: channel.name.clone(),
                    unit: if divide {
                        unit.over(&by)
                    } else {
                        unit.times(&by)
                    }
                    .label(),
                }
            });
        }
        Value::Signal(self.push(Op::Scale { factor, divide }, vec![node], ty, span))
    }

    // Calls.

    /// A call of a function this library does not have, taken for one the
    /// graph the text is bound for may have. Its one signal passes through
    /// as it is, which is the most that can be said of it here. `None` if
    /// it is handed no signal to pass on.
    fn on_trust(&mut self, call: &ast::Call, piped: Option<&Expr>, span: Span) -> Option<Value> {
        let handed = piped.or_else(|| {
            let first = call.args.first()?;
            first.name.is_none().then_some(&first.value)
        })?;
        let Value::Signal(input) = self.expr(handed) else {
            return None;
        };
        // What else it is given is not read, but it is used.
        self.mark_call_used(call);
        let ty = self.ty(input);
        let remark = Diagnostic::remark(
            call.name.span,
            format!("`{}` is not a function known here", call.name.text),
        )
        .label("taken on trust")
        .note(
            "the graph this text is sent to may know it, and says so if it does not; \
             here its result is taken to be its input's rate, channels and units",
        );
        // It may be a slip for one that is known, all the same.
        let names = self.library.functions().map(|b| b.name);
        self.report(match builtins::closest(&call.name.text, names) {
            Some(near) => remark.help(format!("did you mean `{near}`?")),
            None => remark,
        });
        Some(Value::Signal(self.push(
            Op::Call {
                function: remembered(&call.name.text),
                args: Vec::new(),
                arms: Vec::new(),
            },
            vec![input],
            ty,
            span,
        )))
    }

    fn call<'e>(&mut self, call: &'e ast::Call, piped: Option<&'e Expr>, span: Span) -> Value {
        if let Some(function) = self.functions.get(&call.name.text).cloned() {
            return self.expand(&call.name.text, function, call, piped);
        }
        let library = self.library;
        let Some(builtin) = library.find(&call.name.text) else {
            if let Some(&defined) = self.later_functions.get(&call.name.text) {
                // One of the spec's own, called above where it is defined.
                self.forward.insert(call.name.text.clone());
                if let Some(input) = piped {
                    self.mark_used(input);
                }
                self.mark_call_used(call);
                self.report(
                    Diagnostic::error(
                        code::USED_BEFORE_DEFINITION,
                        call.name.span,
                        format!("`{}` is used before it is defined", call.name.text),
                    )
                    .label("used here")
                    .secondary(defined, "defined here")
                    .help("move the definition above its first use"),
                );
                return Value::Poison;
            }
            if library.is_trusting() {
                if let Some(taken) = self.on_trust(call, piped, span) {
                    return taken;
                }
            }
            if let Some(input) = piped {
                self.mark_used(input);
            }
            self.mark_call_used(call);
            let names = || library.functions().map(|b| b.name);
            let diagnostic = Diagnostic::error(
                code::UNKNOWN_FUNCTION,
                call.name.span,
                format!("there is no function `{}`", call.name.text),
            )
            .label("not a function");
            self.report(match builtins::closest(&call.name.text, names()) {
                Some(near) => diagnostic.help(format!("did you mean `{near}`?")),
                None => diagnostic.help(format!("the functions are:\n{}", wrapped(names(), 60))),
            });
            return Value::Poison;
        };
        let written = |builtin: &Builtin| format!("it is written `{}`", builtin.usage);

        // Sort the arguments into signals and the function's own arguments.
        let mut arms: Vec<Arm<'e>> = Vec::new();
        let mut reference: Option<&ast::Arg> = None;
        let mut bound: Vec<Option<&ast::Arg>> = vec![None; builtin.args.len()];
        let mut ok = true;
        if let Some(input) = piped {
            arms.push((input, arm_name(input)));
        }
        for arg in &call.args {
            let Some(name) = &arg.name else {
                let takes_signal = match builtin.signals {
                    Signals::Arms => true,
                    Signals::One => arms.is_empty(),
                };
                if takes_signal {
                    arms.push((&arg.value, arm_name(&arg.value)));
                } else if let Some(free) = bound.iter().position(Option::is_none) {
                    bound[free] = Some(arg);
                } else {
                    self.mark_used(&arg.value);
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENTS,
                            arg.span,
                            format!("`{}` takes no more arguments", builtin.name),
                        )
                        .label("one too many")
                        .help(written(builtin)),
                    );
                    ok = false;
                }
                continue;
            };
            let taken = if builtin.named_signal == Some(name.text.as_str()) {
                reference.replace(arg).is_some()
            } else if let Some(index) = builtin.args.iter().position(|s| s.name == name.text) {
                bound[index].replace(arg).is_some()
            } else if builtin.signals == Signals::Arms {
                arms.push((&arg.value, Some(name.text.clone())));
                false
            } else {
                self.mark_used(&arg.value);
                self.report(
                    Diagnostic::error(
                        code::ARGUMENTS,
                        name.span,
                        format!("`{}` has no argument called `{}`", builtin.name, name.text),
                    )
                    .label("not an argument")
                    .help(written(builtin)),
                );
                ok = false;
                false
            };
            if taken {
                self.report(
                    Diagnostic::error(
                        code::ARGUMENTS,
                        arg.span,
                        format!("`{}` is given twice", name.text),
                    )
                    .label("given again here")
                    .help(written(builtin)),
                );
                ok = false;
            }
        }

        // The signals.
        let mut nodes = Vec::new();
        for (expr, _) in &arms {
            match self.expr(expr) {
                Value::Signal(node) => nodes.push(node),
                Value::Scalar(_) => {
                    let diagnostic = match builtin.signals {
                        Signals::One => Diagnostic::error(
                            code::NO_SIGNAL,
                            call.name.span,
                            format!("`{}` works on a signal, and none was given", builtin.name),
                        )
                        .secondary(expr.span(), "this is a number")
                        .help(format!(
                            "pipe a signal into it: `x | {}`",
                            call.span.text(self.source)
                        )),
                        Signals::Arms => Diagnostic::error(
                            code::WRONG_KIND,
                            expr.span(),
                            format!("`{}` takes signals, and this is a number", builtin.name),
                        )
                        .label("a number"),
                    };
                    self.report(diagnostic);
                    ok = false;
                }
                Value::Poison => ok = false,
            }
        }
        let mut reference_node = None;
        if let Some(arg) = reference {
            match self.expr(&arg.value) {
                Value::Signal(node) => reference_node = Some(node),
                Value::Scalar(_) => {
                    self.report(
                        Diagnostic::error(
                            code::WRONG_KIND,
                            arg.value.span(),
                            format!(
                                "`ref` of `{}` takes a signal, and this is a number",
                                builtin.name
                            ),
                        )
                        .label("a number"),
                    );
                    ok = false;
                }
                Value::Poison => ok = false,
            }
        }

        // The function's own arguments.
        let mut args: Vec<(&ArgSpec, CallArg, Span)> = Vec::new();
        let mut missing: Vec<&str> = Vec::new();
        for (spec, arg) in builtin.args.iter().zip(&bound) {
            match arg {
                Some(arg) => match self.argument(builtin, spec, arg) {
                    Some(value) => args.push((
                        spec,
                        CallArg {
                            name: spec.name,
                            text: arg.value.span().text(self.source).to_owned(),
                            value,
                        },
                        arg.value.span(),
                    )),
                    None => ok = false,
                },
                None if spec.required => missing.push(spec.name),
                None => {}
            }
        }
        if !ok {
            return Value::Poison;
        }

        // Whether enough was given. A function with no signal is reported
        // as that alone, since its other arguments were sorted on the
        // assumption that one was piped in.
        let enough = match builtin.signals {
            Signals::One => !nodes.is_empty(),
            Signals::Arms => nodes.len() >= 2,
        };
        if !enough {
            self.report(match builtin.signals {
                Signals::One => Diagnostic::error(
                    code::NO_SIGNAL,
                    call.name.span,
                    format!("`{}` works on a signal, and none was given", builtin.name),
                )
                .help(format!(
                    "pipe a signal into it: `x | {}`",
                    call.span.text(self.source)
                )),
                Signals::Arms => Diagnostic::error(
                    code::ARGUMENTS,
                    call.span,
                    format!("`{}` takes at least two signals", builtin.name),
                )
                .label(format!("only {} given", nodes.len()))
                .help(written(builtin)),
            });
            return Value::Poison;
        }
        if let (Some(name), None) = (builtin.named_signal, reference_node) {
            missing.insert(0, name);
        }
        if !missing.is_empty() {
            let list: Vec<String> = missing.iter().map(|name| format!("`{name}`")).collect();
            self.report(
                Diagnostic::error(
                    code::ARGUMENTS,
                    call.span,
                    format!("`{}` needs {}", builtin.name, list.join(" and ")),
                )
                .label("not given")
                .help(written(builtin)),
            );
            return Value::Poison;
        }
        // Every function here works on samples in time. A spectrum has
        // none, and its "rate" is bins per hertz, which would pass for one.
        if let Some(position) = nodes
            .iter()
            .position(|&node| self.program.is_spectrum(node))
        {
            self.report(
                Diagnostic::error(
                    code::DOMAINS,
                    arms[position].0.span(),
                    format!(
                        "`{}` works on a signal in time, and this is a spectrum",
                        builtin.name
                    ),
                )
                .label(self.program.describe(self.ty(nodes[position]).domain))
                .help("apply it before `psd`"),
            );
            return Value::Poison;
        }

        if !self.within_limits(builtin, &args, call, arms[0].0, nodes[0]) {
            return Value::Poison;
        }

        // The result.
        let input = self.ty(nodes[0]);
        let mut inputs = nodes.clone();
        let mut arm_names = Vec::new();
        let integer = |name: &str| {
            args.iter().find_map(|(_, arg, _)| match arg.value {
                ArgValue::Integer(value) if arg.name == name => Some(value),
                _ => None,
            })
        };
        let ty = match builtin.rule {
            Rule::Same => input,
            Rule::Integrates | Rule::Differentiates => {
                let second = UnitExpr::symbol("s");
                SignalType {
                    domain: input.domain,
                    channels: relabel(&input.channels, |channel| {
                        let unit = UnitExpr::of(&channel.unit);
                        Channel {
                            name: channel.name.clone(),
                            unit: if builtin.rule == Rule::Integrates {
                                unit.times(&second)
                            } else {
                                unit.over(&second)
                            }
                            .label(),
                        }
                    }),
                }
            }
            Rule::Spectrum => {
                let seconds = args
                    .iter()
                    .find_map(|(_, arg, _)| match arg.value {
                        ArgValue::Number(seconds) if arg.name == "window" => Some(seconds),
                        _ => None,
                    })
                    .expect("`window` is required");
                let rate = self.program.domain(input.domain).rate;
                let samples = crate::nodes::samples_in(seconds, rate);
                if crate::spectrum::shape(samples, rate).is_none() {
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENT_VALUE,
                            call.span,
                            "`window` of `psd` is too short for a spectrum",
                        )
                        .label(format!("{samples} samples at {}", format_rate(rate)))
                        .help("a spectrum takes at least 60 samples"),
                    );
                    return Value::Poison;
                }
                let hertz = UnitExpr::symbol("Hz");
                SignalType {
                    domain: self.derive(input.domain, Derived::Spectrum(samples)),
                    channels: relabel(&input.channels, |channel| Channel {
                        name: channel.name.clone(),
                        unit: UnitExpr::of(&channel.unit).pow(2).over(&hertz).label(),
                    }),
                }
            }
            Rule::Magnitude => {
                let first = input.channels.get(0).expect("a channel set is never empty");
                if let Some(other) = input.channels.iter().find(|c| c.unit != first.unit) {
                    self.report(
                        Diagnostic::error(
                            code::UNITS,
                            arms[0].0.span(),
                            "`magnitude` combines channels that share a unit",
                        )
                        .label(format!(
                            "`{}` is in {}, and `{}` is in {}",
                            first.name, first.unit, other.name, other.unit
                        )),
                    );
                    return Value::Poison;
                }
                SignalType {
                    domain: input.domain,
                    channels: ChannelSet::new(vec![Channel {
                        name: "magnitude".to_owned(),
                        unit: first.unit.clone(),
                    }])
                    .expect("one channel is a channel set"),
                }
            }
            Rule::Decimate => {
                let factor = integer("n").expect("`n` is required");
                SignalType {
                    domain: self.derive(input.domain, Derived::Decimate(factor)),
                    channels: input.channels,
                }
            }
            Rule::Resample => {
                let rate = args
                    .iter()
                    .find_map(|(_, arg, _)| match arg.value {
                        ArgValue::Number(rate) => Some(rate),
                        _ => None,
                    })
                    .expect("`rate` is required");
                SignalType {
                    domain: self.derive(input.domain, Derived::Resample(rate.to_bits())),
                    channels: input.channels,
                }
            }
            Rule::Align => match self.align(&arms, &nodes) {
                Some((ty, names)) => {
                    arm_names = names;
                    ty
                }
                None => return Value::Poison,
            },
            Rule::Decorrelate => {
                let other = reference_node.expect("`ref` is required");
                if self.ty(other).domain != input.domain {
                    let span = reference.map_or(call.span, |arg| arg.value.span());
                    self.report(
                        Diagnostic::error(
                            code::DOMAINS,
                            span,
                            "cannot decorrelate signals in different rate domains",
                        )
                        .label(self.rate_label(other))
                        .secondary(arms[0].0.span(), self.rate_label(nodes[0]))
                        .help("bring them into one domain first, with `align(…)`"),
                    );
                    return Value::Poison;
                }
                inputs.push(other);
                input
            }
        };
        let args = args.into_iter().map(|(_, arg, _)| arg).collect();
        Value::Signal(self.push(
            Op::Call {
                function: builtin.name,
                args,
                arms: arm_names,
            },
            inputs,
            ty,
            span,
        ))
    }

    /// Checks one argument against what the function expects there.
    fn argument(&mut self, builtin: &Builtin, spec: &ArgSpec, arg: &ast::Arg) -> Option<ArgValue> {
        let span = arg.value.span();
        let what = format!("`{}` of `{}`", spec.name, builtin.name);
        let fixed = "it fixes the shape of the graph, so it cannot be a param or be computed";
        match spec.kind {
            ArgKind::Frequency | ArgKind::Time | ArgKind::Number => {
                let scalar = match self.expr(&arg.value) {
                    Value::Scalar(scalar) => scalar,
                    Value::Signal(_) => {
                        self.report(
                            Diagnostic::error(
                                code::WRONG_KIND,
                                span,
                                format!("{what} takes a number, and this is a signal"),
                            )
                            .label("a signal"),
                        );
                        return None;
                    }
                    Value::Poison => return None,
                };
                // A frequency is anything that converts to hertz, and a
                // duration anything that converts to seconds, so `1.5kHz`,
                // `250ms` and one over a time all do.
                let (base, kind, example) = match spec.kind {
                    ArgKind::Frequency => ("Hz", "a frequency", "`60`, `60Hz` or `1.5kHz`"),
                    ArgKind::Time => ("s", "a duration", "`10s` or `250ms`"),
                    _ => ("", "a plain number", "a number without a unit"),
                };
                let to = UnitExpr::parse(base).expect("a base unit");
                let factor = if scalar.unit.is_none() {
                    1.0
                } else {
                    match UnitExpr::of(&scalar.unit).convert(&to) {
                        Ok(factor) => factor,
                        Err(error) => {
                            self.report(
                                Diagnostic::error(
                                    code::UNITS,
                                    span,
                                    format!("{what} is {kind}: {error}"),
                                )
                                .label(format!("in {}", scalar.unit))
                                .help(format!("write {example}")),
                            );
                            return None;
                        }
                    }
                };
                Some(ArgValue::Scalar(Scalar {
                    expr: scaled(scalar.expr, factor),
                    unit: Unit::new(base),
                }))
            }
            ArgKind::Rate => {
                let rate = match &arg.value {
                    Expr::Number(number) if number.value > 0.0 => in_hertz(number).ok(),
                    _ => None,
                };
                if rate.is_none() {
                    self.mark_used(&arg.value);
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENT_VALUE,
                            span,
                            format!("{what} is a rate written out, such as `500` or `1kHz`"),
                        )
                        .note(fixed),
                    );
                }
                rate.map(ArgValue::Number)
            }
            ArgKind::Span => {
                let seconds = match &arg.value {
                    Expr::Number(number) if number.value > 0.0 => in_seconds(number).ok(),
                    _ => None,
                };
                if seconds.is_none() {
                    self.mark_used(&arg.value);
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENT_VALUE,
                            span,
                            format!("{what} is a duration written out, such as `2 s` or `500ms`"),
                        )
                        .note(fixed),
                    );
                }
                seconds.map(ArgValue::Number)
            }
            ArgKind::Integer { min } => match &arg.value {
                Expr::Number(number)
                    if number.unit.is_none()
                        && number.value.fract() == 0.0
                        && number.value >= min as f64
                        && number.value <= 1e9 =>
                {
                    Some(ArgValue::Integer(number.value as i64))
                }
                _ => {
                    self.mark_used(&arg.value);
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENT_VALUE,
                            span,
                            format!("{what} is a whole number of at least {min}, written out"),
                        )
                        .note(fixed),
                    );
                    None
                }
            },
            ArgKind::Choice(options) => {
                let chosen = match &arg.value {
                    Expr::Name(name) => options.iter().find(|option| **option == name.text),
                    _ => None,
                };
                if chosen.is_none() {
                    self.report(Diagnostic::error(
                        code::ARGUMENT_VALUE,
                        span,
                        format!("{what} is one of: {}", options.join(", ")),
                    ));
                }
                chosen.copied().map(ArgValue::Choice)
            }
        }
    }

    /// Checks that tunable arguments stay positive, and frequencies below
    /// the Nyquist limit of the signal, over the whole range of their params.
    fn within_limits(
        &mut self,
        builtin: &Builtin,
        args: &[(&ArgSpec, CallArg, Span)],
        call: &ast::Call,
        input: &Expr,
        node: usize,
    ) -> bool {
        let mut ok = true;
        let rate = self.program.rate_of(node);
        let nyquist = rate / 2.0;
        for (spec, arg, span) in args {
            let ArgValue::Scalar(scalar) = &arg.value else {
                continue;
            };
            let (low, high) = self.program.bounds(&scalar.expr);
            let what = format!("`{}` of `{}`", arg.name, builtin.name);
            if low.is_nan() || low <= 0.0 {
                let label = if low == high {
                    format!("this is {low}")
                } else {
                    format!("this can be as low as {low}")
                };
                self.report(
                    Diagnostic::error(
                        code::ARGUMENT_VALUE,
                        *span,
                        format!("{what} must be positive"),
                    )
                    .label(label),
                );
                ok = false;
            } else if spec.kind == ArgKind::Frequency && high >= nyquist {
                let label = if low == high {
                    format!("this is {}", format_rate(high))
                } else {
                    format!("this can be as high as {}", format_rate(high))
                };
                self.report(
                    Diagnostic::error(
                        code::NYQUIST,
                        *span,
                        format!("{what} reaches the Nyquist frequency of its signal"),
                    )
                    .label(label)
                    .secondary(
                        input.span(),
                        format!(
                            "sampled at {}, so the limit is {}",
                            format_rate(rate),
                            format_rate(nyquist)
                        ),
                    )
                    .help("lower the frequency or its range, or filter ahead of `decimate`"),
                );
                ok = false;
            }
        }
        if ok && builtin.name == "bandpass" {
            let value = |name: &str| {
                args.iter().find_map(|(_, arg, _)| match &arg.value {
                    ArgValue::Scalar(scalar) if arg.name == name => {
                        Some(self.program.value(&scalar.expr))
                    }
                    _ => None,
                })
            };
            if let (Some(lo), Some(hi)) = (value("lo"), value("hi")) {
                if lo >= hi {
                    self.report(
                        Diagnostic::error(
                            code::ARGUMENT_VALUE,
                            call.span,
                            "`lo` of `bandpass` must be below `hi`",
                        )
                        .label(format!("{lo} Hz is not below {hi} Hz")),
                    );
                    ok = false;
                }
            }
        }
        ok
    }

    fn derive(&mut self, from: DomainId, how: Derived) -> DomainId {
        if let Some(&domain) = self.derived.get(&(from, how)) {
            return domain;
        }
        let rate = self.program.domain(from).rate;
        let domain = match how {
            Derived::Decimate(factor) => {
                self.new_domain(rate / factor as f64, Origin::Decimated { from, factor })
            }
            Derived::Resample(bits) => {
                self.new_domain(f64::from_bits(bits), Origin::Resampled { from })
            }
            Derived::Spectrum(samples) => {
                let (bins, resolution) = crate::spectrum::shape(samples, rate)
                    .expect("the window was found long enough before its domain is made");
                self.new_domain(
                    1.0 / resolution,
                    Origin::Spectrum {
                        from,
                        bins,
                        resolution,
                    },
                )
            }
        };
        self.derived.insert((from, how), domain);
        domain
    }

    /// Merges arms into one domain. Their rates must already match.
    fn align(&mut self, arms: &[Arm<'_>], nodes: &[usize]) -> Option<(SignalType, Vec<String>)> {
        let mut names: Vec<String> = Vec::new();
        for (expr, name) in arms {
            let Some(name) = name else {
                self.report(
                    Diagnostic::error(
                        code::ARGUMENTS,
                        expr.span(),
                        "this arm of `align` has no name",
                    )
                    .label("what are its channels called?")
                    .note("an arm's channels are named after it, as in `left.x`")
                    .help("name it: `align(left = …, right = …)`"),
                );
                return None;
            };
            if let Some(first) = names.iter().position(|n| n == name) {
                self.report(
                    Diagnostic::error(
                        code::ARGUMENTS,
                        expr.span(),
                        format!("two arms of `align` are called `{name}`"),
                    )
                    .label("the second")
                    .secondary(arms[first].0.span(), "the first")
                    .help("name them apart: `align(a = …, b = …)`"),
                );
                return None;
            }
            names.push(name.clone());
        }

        let rates: Vec<f64> = nodes.iter().map(|&n| self.program.rate_of(n)).collect();
        if let Some(odd) = rates.iter().position(|rate| *rate != rates[0]) {
            let mut diagnostic = Diagnostic::error(
                code::ALIGN_RATES,
                arms[odd].0.span(),
                "cannot align signals in different rate domains",
            )
            .label(self.rate_label(nodes[odd]));
            for (index, (expr, _)) in arms.iter().enumerate() {
                if index != odd {
                    diagnostic = diagnostic.secondary(expr.span(), self.rate_label(nodes[index]));
                }
            }
            // Rewrite the definition with each faster arm brought down to
            // the slowest rate, as a fix to copy.
            let slowest = rates.iter().copied().fold(f64::INFINITY, f64::min);
            let mut fixed = self.item.text(self.source).to_owned();
            let mut ends: Vec<(usize, f64)> = arms
                .iter()
                .zip(&rates)
                .filter(|(_, rate)| **rate > slowest)
                .map(|((expr, _), rate)| (expr.span().end - self.item.start, *rate))
                .collect();
            ends.sort_by_key(|(end, _)| std::cmp::Reverse(*end));
            for (end, rate) in ends {
                let ratio = rate / slowest;
                let stage = if (ratio - ratio.round()).abs() < 1e-9 {
                    format!(" | decimate({})", ratio.round())
                } else {
                    format!(" | resample({slowest})")
                };
                fixed.insert_str(end, &stage);
            }
            let fixed: Vec<String> = fixed.lines().map(|line| format!("  {line}")).collect();
            self.report(
                diagnostic
                    .note(
                        "alignment never resamples on its own — interpolating without\n\
                         being asked would change your numbers invisibly.",
                    )
                    .help(format!(
                        "match the rate first, ahead of any filtering:\n{}",
                        fixed.join("\n")
                    ))
                    .note(
                        "decimating after the filters would compile, but the arms would\n\
                         carry different coefficients and mismatched phase.",
                    ),
            );
            return None;
        }

        let mut channels = Vec::new();
        for (name, &node) in names.iter().zip(nodes) {
            for channel in self.ty(node).channels.iter() {
                channels.push(Channel {
                    name: format!("{name}.{}", channel.name),
                    unit: channel.unit.clone(),
                });
            }
        }
        let domain = self.new_domain(
            rates[0],
            Origin::Aligned {
                arms: names.clone(),
            },
        );
        let channels = ChannelSet::new(channels).expect("arm names keep channels apart");
        Some((SignalType { domain, channels }, names))
    }
}

/// What `align` calls an arm when it is not named, which is the name the
/// expression starts from.
fn arm_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Name(name) | Expr::Select { name, .. } => Some(name.text.clone()),
        Expr::Pipe { input: inner, .. }
        | Expr::Paren { inner, .. }
        | Expr::In { value: inner, .. } => arm_name(inner),
        _ => None,
    }
}

/// A comma-separated list broken into lines of at most `width` characters.
fn wrapped<'a>(items: impl Iterator<Item = &'a str>, width: usize) -> String {
    let mut lines = vec![String::new()];
    for item in items {
        let line = lines.last_mut().expect("there is always a line");
        if line.is_empty() {
            line.push_str(item);
        } else if line.chars().count() + item.chars().count() + 2 <= width {
            line.push_str(", ");
            line.push_str(item);
        } else {
            line.push(',');
            lines.push(item.to_owned());
        }
    }
    lines.join("\n")
}

/// The label for a unit written on a number. It is the unit's one spelling
/// if the text reads as a unit, and the text itself if it does not.
fn canonical(written: &str) -> Unit {
    UnitExpr::parse(written).map_or_else(|| Unit::new(written), |unit| unit.label())
}

/// A number written as a rate, in hertz.
fn in_hertz(number: &ast::Number) -> Result<f64, units::Inconvertible> {
    let hertz = UnitExpr::symbol("Hz");
    match &number.unit {
        None => Ok(number.value),
        Some(unit) => Ok(number.value * UnitExpr::of(&canonical(unit)).convert(&hertz)?),
    }
}

/// A number written as a duration, in seconds.
fn in_seconds(number: &ast::Number) -> Result<f64, units::Inconvertible> {
    let second = UnitExpr::symbol("s");
    match &number.unit {
        None => Ok(number.value),
        Some(unit) => Ok(number.value * UnitExpr::of(&canonical(unit)).convert(&second)?),
    }
}

/// A scalar times a constant, left alone when the constant is one.
fn scaled(expr: ScalarExpr, factor: f64) -> ScalarExpr {
    if factor == 1.0 {
        expr
    } else {
        ScalarExpr::Binary(
            BinaryOp::Mul,
            Box::new(expr),
            Box::new(ScalarExpr::Number(factor)),
        )
    }
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Signal(_) => "a signal",
        Value::Scalar(_) => "a number",
        Value::Poison => "unknown",
    }
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The same channels with each rewritten by `change`, which keeps names.
fn relabel(channels: &ChannelSet, change: impl Fn(&Channel) -> Channel) -> Arc<ChannelSet> {
    ChannelSet::new(channels.iter().map(change).collect()).expect("names are unchanged")
}

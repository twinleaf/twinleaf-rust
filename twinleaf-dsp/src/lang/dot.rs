//! Draws a checked program as a Graphviz graph, for a quick look.

use super::check::{ArgValue, CallArg, Op, Program, ScalarExpr};
use std::fmt::Write;

/// Writes `program` in DOT. `name` becomes the graph's name.
pub fn dot(program: &Program, name: &str) -> String {
    let mut out = String::new();
    writeln!(out, "digraph {} {{", quote(name)).unwrap();
    writeln!(out, "  rankdir=LR;").unwrap();
    writeln!(out, "  node [shape=box, fontname=\"Helvetica\"];").unwrap();
    writeln!(out, "  edge [fontname=\"Helvetica\", fontsize=10];").unwrap();

    for (index, node) in program.nodes.iter().enumerate() {
        let channels = node.ty.channels.len();
        let rate = program.sampling_of(index);
        let (label, shape) = match &node.op {
            Op::Source { source } => (
                format!("{}\n{channels} ch · {rate}", program.sources[*source].path),
                ", style=rounded",
            ),
            Op::Select { name } => (format!(".{name}"), ""),
            Op::Call { function, args, .. } => {
                let args = arguments(args);
                if args.is_empty() {
                    ((*function).to_owned(), "")
                } else {
                    (format!("{function}({})", args.join(", ")), "")
                }
            }
            Op::Add => ("+".to_owned(), ""),
            Op::Subtract => ("−".to_owned(), ""),
            Op::Scale { factor, divide } => {
                let sign = if *divide { "÷" } else { "×" };
                (format!("{sign} {}", scalar(program, &factor.expr)), "")
            }
            Op::Negate => ("negate".to_owned(), ""),
            Op::Declare { unit } => (format!("in {unit}"), ""),
        };
        writeln!(out, "  n{index} [label={}{shape}];", quote(&label)).unwrap();
    }
    for (index, output) in program.outputs.iter().enumerate() {
        let ty = &program.nodes[output.node].ty;
        let mut label = format!("/255/{}\n{} ch", output.name, ty.channels.len());
        let unit = &ty
            .channels
            .get(0)
            .expect("a channel set is never empty")
            .unit;
        if !unit.is_none() && ty.channels.iter().all(|c| c.unit == *unit) {
            write!(label, " · {unit}").unwrap();
        }
        writeln!(
            out,
            "  out{index} [label={}, style=\"rounded,bold\"];",
            quote(&label)
        )
        .unwrap();
    }

    let edge = |from: usize| {
        let ty = &program.nodes[from].ty;
        format!(
            "D{} · {} · {} ch",
            ty.domain.0,
            program.sampling_of(from),
            ty.channels.len()
        )
    };
    for (index, node) in program.nodes.iter().enumerate() {
        for &input in &node.inputs {
            writeln!(
                out,
                "  n{input} -> n{index} [label={}];",
                quote(&edge(input))
            )
            .unwrap();
        }
    }
    for (index, output) in program.outputs.iter().enumerate() {
        let from = output.node;
        writeln!(
            out,
            "  n{from} -> out{index} [label={}];",
            quote(&edge(from))
        )
        .unwrap();
    }
    writeln!(out, "}}").unwrap();
    out
}

/// A call's arguments, each as it reads best.
pub(super) fn arguments(args: &[CallArg]) -> Vec<String> {
    args.iter()
        .enumerate()
        .map(|(position, arg)| {
            // The leading arguments read well bare, as written in a
            // pipeline. The rest are clearer with names.
            let bare = position == 0
                || matches!(arg.name, "hi")
                || matches!(arg.value, ArgValue::Choice(_));
            if bare {
                arg.text.clone()
            } else {
                format!("{} = {}", arg.name, arg.text)
            }
        })
        .collect()
}

/// A scalar as a short formula, with params by name.
pub(super) fn scalar(program: &Program, expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Number(value) => value.to_string(),
        ScalarExpr::Param(index) => program.params[*index].name.clone(),
        ScalarExpr::Negate(operand) => format!("-{}", scalar(program, operand)),
        ScalarExpr::Binary(op, left, right) => format!(
            "({} {} {})",
            scalar(program, left),
            op.symbol(),
            scalar(program, right)
        ),
    }
}

/// A DOT string literal.
fn quote(text: &str) -> String {
    let escaped = text
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

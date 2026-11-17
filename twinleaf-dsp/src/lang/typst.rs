//! Draws a checked program as a Typst figure, for a paper or a notebook.
//!
//! The figure is laid out by the [autograph] package, which measures the
//! nodes, has Graphviz place them and draws the result with fletcher. What
//! is written here is checked against autograph 0.1.0. There a node is named
//! by a label and may carry content, an edge names its two nodes and takes
//! no label of its own, and the layout runs the way Graphviz runs it, from
//! the top down. So what a signal is, its rate and channels and unit, is
//! said on the node that makes it, wherever that differs from what came in.
//!
//! [autograph]: https://typst.app/universe/package/autograph/

use super::check::{Op, Program};
use super::dot::{arguments, scalar};
use std::fmt::Write;

/// The packages the figure is written against. A later version may spell
/// things otherwise, so the versions are pinned here and nowhere else.
pub const AUTOGRAPH: &str = "@preview/autograph:0.1.0";
pub const FLETCHER: &str = "@preview/fletcher:0.5.8";

/// Writes `program` as a Typst figure, which is the imports it needs and
/// one diagram. `name` is noted at the top. It sets nothing about the page
/// or the text, so it can be included in a document as it is.
pub fn figure(program: &Program, name: &str) -> String {
    let mut out = String::new();
    writeln!(out, "// {name}, a signal graph drawn by twinleaf-dsp.").unwrap();
    writeln!(out, "#import \"{AUTOGRAPH}\": diagram, node, edge").unwrap();
    writeln!(out, "#import \"{FLETCHER}\": shapes").unwrap();
    writeln!(out).unwrap();
    // Drawn as curves, an edge takes its own stroke and arrowhead and none
    // from the diagram, and the ones it comes with are heavy.
    writeln!(
        out,
        "#let wire = (stroke: 0.6pt + black, mark: (end: \">\", fill: black, scale: 0.8))"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "#diagram(").unwrap();
    writeln!(out, "  bezier: true,").unwrap();
    writeln!(out, "  node-shape: shapes.rect,").unwrap();
    writeln!(out, "  node-stroke: 0.6pt + black,").unwrap();
    writeln!(out, "  node-inset: 6pt,").unwrap();
    writeln!(out, "  node-corner-radius: 2pt,").unwrap();
    writeln!(out).unwrap();

    for (index, node) in program.nodes.iter().enumerate() {
        // What it is, as it would be written.
        let (what, source) = match &node.op {
            Op::Source { source } => (program.sources[*source].path.to_string(), true),
            Op::Select { name } => (format!(".{name}"), false),
            Op::Call { function, args, .. } => {
                let args = arguments(args);
                let what = match args.is_empty() {
                    true => (*function).to_owned(),
                    false => format!("{function}({})", args.join(", ")),
                };
                (what, false)
            }
            Op::Add => ("+".to_owned(), false),
            Op::Subtract => ("−".to_owned(), false),
            Op::Scale { factor, divide } => {
                let sign = if *divide { "÷" } else { "×" };
                (format!("{sign} {}", scalar(program, &factor.expr)), false)
            }
            Op::Negate => ("negate".to_owned(), false),
            Op::Declare { unit } => (format!("in {unit}"), false),
        };
        // What it carries, where that is news: at a source, and wherever
        // the rate, the channels or the unit are not the input's.
        let signal = program.signal_of(index);
        let news = node.inputs.is_empty()
            || node
                .inputs
                .iter()
                .any(|&input| program.signal_of(input) != signal);
        let mut body = format!("#raw({})", string(&what));
        if news {
            write!(body, " \\ #text(0.8em, {})", string(&signal)).unwrap();
        }
        let style = if source { ", shape: shapes.pill" } else { "" };
        writeln!(out, "  node(<n{index}>, align(center)[{body}]{style}),").unwrap();
    }
    for (index, output) in program.outputs.iter().enumerate() {
        let body = format!(
            "#strong(raw({})) \\ #text(0.8em, {})",
            string(&format!("/255/{}", output.name)),
            string(&program.signal_of(output.node))
        );
        writeln!(
            out,
            "  node(<out{index}>, align(center)[{body}], shape: shapes.pill, stroke: 1pt + black),"
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    for (index, node) in program.nodes.iter().enumerate() {
        for &input in &node.inputs {
            writeln!(out, "  edge(<n{input}>, <n{index}>, ..wire),").unwrap();
        }
    }
    for (index, output) in program.outputs.iter().enumerate() {
        writeln!(out, "  edge(<n{}>, <out{index}>, ..wire),", output.node).unwrap();
    }
    writeln!(out, ")").unwrap();
    out
}

/// The figure as a document of its own, on a page cut to the figure, in a
/// plain face. This is what is compiled when a PDF or an SVG is asked for.
pub fn document(program: &Program, name: &str) -> String {
    format!(
        "#set page(width: auto, height: auto, margin: 8pt)\n#set text(size: 9pt)\n\n{}",
        figure(program, name)
    )
}

/// A Typst string literal. Everything in the figure that came from the
/// spec is written as one, so that nothing in a name or a unit is read as
/// markup.
fn string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_is_written_so_typst_reads_it_back() {
        assert_eq!(string("nT/V"), "\"nT/V\"");
        assert_eq!(string("a \"b\" \\ c"), "\"a \\\"b\\\" \\\\ c\"");
        assert_eq!(string("m/s^2 · µT"), "\"m/s^2 · µT\"");
    }
}

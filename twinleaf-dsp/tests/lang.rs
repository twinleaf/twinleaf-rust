//! The language front end: the syntax tree loses nothing, edits are splices,
//! and the example spec checks into the program the plan describes.

use std::fs;
use std::path::PathBuf;
use twinleaf_dsp::lang::ast::{self, BinaryOp, Expr, Item};
use twinleaf_dsp::lang::check::{ArgValue, Op, Origin, ScalarExpr};
use twinleaf_dsp::lang::syntax::{SyntaxElement, SyntaxKind, SyntaxNode};
use twinleaf_dsp::lang::{self, parser, Span};
use twinleaf_dsp::Unit;

mod common;

fn crate_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn example() -> String {
    fs::read_to_string(crate_file("examples/gradiometer.tsl")).unwrap()
}

/// Every spec kept in the repository, right or wrong.
fn specs() -> Vec<String> {
    let mut specs = vec![example()];
    for entry in fs::read_dir(crate_file("tests/diagnostics")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "tsl") {
            specs.push(fs::read_to_string(path).unwrap());
        }
    }
    specs
}

/// Text assembled at random from pieces of the language, most of it wrong.
fn soup(count: usize) -> Vec<String> {
    const PIECES: [&str; 34] = [
        "source ",
        "param ",
        "let ",
        "out ",
        " ",
        "\n",
        "a",
        "b",
        "clean",
        " = ",
        "|",
        "(",
        ")",
        ",",
        ".",
        "..",
        "/",
        "/0/field",
        "0",
        "field",
        "@rate(1000)",
        "@channels(x, y)",
        "[",
        "]",
        "-",
        "+",
        "*",
        ":",
        "10s",
        "1e3",
        "# note\n",
        "$",
        "µ",
        "notch(60)",
    ];
    let mut state = 0x7157_u64;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) as usize
    };
    (0..count)
        .map(|_| {
            (0..1 + next() % 40)
                .map(|_| PIECES[next() % PIECES.len()])
                .collect()
        })
        .collect()
}

/// Checks that a node's span is exactly its own text: it begins and ends at a
/// token the parser did not skip.
fn assert_tight(node: &SyntaxNode, source: &str) {
    let tokens = node.all_tokens();
    let significant: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    if let (Some(first), Some(last)) = (significant.first(), significant.last()) {
        assert_eq!(
            node.span,
            Span::new(first.span.start, last.span.end),
            "{:?} `{}` is not tight in {source:?}",
            node.kind,
            node.span.text(source)
        );
    }
    for child in node.nodes() {
        assert_tight(child, source);
    }
}

#[test]
fn the_tree_holds_every_byte_of_any_text() {
    let mut texts = specs();
    texts.extend(soup(2000));
    texts.extend(["", " ", "#", "let", "out x = ((((", "source a = /"].map(String::from));
    for source in &texts {
        let parse = parser::parse(source);
        assert_eq!(&parse.root.text(source), source);

        let mut at = 0;
        for token in parse.root.all_tokens() {
            assert_eq!(token.span.start, at, "tokens leave a gap in {source:?}");
            at = token.span.end;
        }
        assert_eq!(at, source.len());

        assert_eq!(parse.root.span, Span::new(0, source.len()));
        for item in parse.root.nodes() {
            assert_tight(item, source);
        }
        // Whatever the text, checking it reports and does not panic.
        lang::analyze(source);
    }
}

#[test]
fn an_edit_is_a_splice_and_leaves_the_rest_alone() {
    let source = example();
    let before = lang::analyze(&source);
    let line = before
        .program
        .params
        .iter()
        .find(|p| p.name == "line")
        .unwrap();
    assert_eq!(line.value_span.text(&source), "60");

    // Dragging a slider: replace the literal's span.
    let mut tuned = source.clone();
    tuned.replace_range(line.value_span.start..line.value_span.end, "55");
    let after = lang::analyze(&tuned);
    assert!(after.diagnostics.is_empty());
    assert_eq!(after.program.params[0].value, 55.0);
    assert_eq!(after.program.params[1..], before.program.params[1..]);
    assert_eq!(tuned.len(), source.len());
    assert_eq!(tuned.replace("= 55 ", "= 60 "), source);

    // Dropping a node onto an edge: splice a stage in after the pipeline
    // that the caret is in.
    let caret = source.find("notch(line").unwrap();
    let call = before.syntax.node_at(caret);
    assert_eq!(call.kind, SyntaxKind::Call);
    assert_eq!(call.span.text(&source), "notch(line, q = 30)");
    let mut extended = source.clone();
    extended.insert_str(call.span.end, " | detrend(mean)");
    let after = lang::analyze(&extended);
    assert!(after.diagnostics.is_empty());
    assert_eq!(after.program.nodes.len(), before.program.nodes.len() + 1);
    // Every comment is still there, because nothing was regenerated.
    assert_eq!(extended.matches('#').count(), source.matches('#').count());
}

#[test]
fn arithmetic_binds_tighter_than_a_pipe() {
    let source = "let y = a - b * 2 | lowpass(10) | rms(1s)";
    let file = ast::lower(&parser::parse(source).root, source);
    let Item::Let(item) = &file.items[0] else {
        panic!("not a let");
    };
    // ((a - (b * 2)) | lowpass(10)) | rms(1s)
    let Expr::Pipe { input, call, .. } = &item.value else {
        panic!("not a pipeline");
    };
    assert_eq!(call.name.text, "rms");
    let Expr::Pipe { input, call, .. } = &**input else {
        panic!("not a pipeline");
    };
    assert_eq!(call.name.text, "lowpass");
    let Expr::Binary {
        op: BinaryOp::Sub,
        right,
        ..
    } = &**input
    else {
        panic!("not a difference");
    };
    assert!(matches!(
        &**right,
        Expr::Binary {
            op: BinaryOp::Mul,
            ..
        }
    ));
}

#[test]
fn the_gradiometer_checks_into_the_planned_graph() {
    let source = example();
    let analysis = lang::analyze(&source);
    assert_eq!(analysis.diagnostics, []);
    let program = &analysis.program;

    // Two streams, the decimated arm, and the domain align makes of them.
    let rates: Vec<f64> = program.domains.iter().map(|d| d.rate).collect();
    assert_eq!(rates, [1000.0, 500.0, 500.0, 500.0]);
    assert!(matches!(
        program.domains[2].origin,
        Origin::Decimated { factor: 2, .. }
    ));
    assert_eq!(
        program.domains[3].origin,
        Origin::Aligned {
            arms: vec!["a".into(), "b".into()]
        }
    );

    // align names each arm's channels after the arm.
    let aligned = program
        .nodes
        .iter()
        .find(|n| {
            matches!(
                n.op,
                Op::Call {
                    function: "align",
                    ..
                }
            )
        })
        .unwrap();
    let names: Vec<&str> = aligned
        .ty
        .channels
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["a.x", "a.y", "a.z", "b.x", "b.y", "b.z"]);

    // The filters run once, over all six channels, in the aligned domain.
    for function in ["notch", "bandpass"] {
        let filter = program
            .nodes
            .iter()
            .find(|n| matches!(n.op, Op::Call { function: f, .. } if f == function))
            .unwrap();
        assert_eq!(filter.ty, aligned.ty);
    }
    let notch = program
        .nodes
        .iter()
        .find_map(|n| match &n.op {
            Op::Call {
                function: "notch",
                args,
                ..
            } => Some(args),
            _ => None,
        })
        .unwrap();
    assert_eq!(notch[0].name, "f");
    assert!(matches!(
        &notch[0].value,
        ArgValue::Scalar(scalar) if scalar.expr == ScalarExpr::Param(0)
    ));

    // The output pairs x with x, y with y, z with z, in nT at 500 Hz.
    let [gradient] = program.outputs.as_slice() else {
        panic!("one output");
    };
    assert_eq!(gradient.name, "gradient");
    let node = &program.nodes[gradient.node];
    assert_eq!(node.op, Op::Subtract);
    assert_eq!(program.rate_of(gradient.node), 500.0);
    let channels: Vec<(&str, &Unit)> = node
        .ty
        .channels
        .iter()
        .map(|c| (c.name.as_str(), &c.unit))
        .collect();
    let nt = Unit::new("nT");
    assert_eq!(channels, [("x", &nt), ("y", &nt), ("z", &nt)]);

    // Each param keeps its value, its range, and its unit.
    let params: Vec<_> = program
        .params
        .iter()
        .map(|p| (p.name.as_str(), p.value, p.range, p.unit.symbol()))
        .collect();
    assert_eq!(
        params,
        [
            ("line", 60.0, Some((45.0, 65.0)), "Hz"),
            ("lo", 1.0, Some((0.1, 10.0)), "Hz"),
            ("hi", 40.0, Some((10.0, 200.0)), "Hz"),
        ]
    );
}

#[test]
fn one_broken_definition_is_one_diagnostic() {
    let source = "\
source a = /0/field @rate(1000) @channels(x, y, z)
let clean = a | notch(
let more = clean | lowpass(10)
out y = more.x - clean.x
";
    let analysis = lang::analyze(source);
    let codes: Vec<_> = analysis.diagnostics.iter().map(|d| d.code).collect();
    assert_eq!(codes, [Some("E0002")]);
    assert!(analysis.has_errors());
    assert!(analysis.program.outputs.is_empty());
}

#[test]
fn units_multiply_divide_and_convert_only_when_asked() {
    let source = "\
source v = /0/coil @rate(1000) @channels(x, y) @unit(V)
param gain : nT/V = 2.5
out scaled = v * 3
out field : nT = v * gain
out flux = v | integrate()
out slope = v | derivative()
out small = v in mV
out ratio = v / (2 V)
";
    let analysis = lang::analyze(source);
    assert_eq!(analysis.diagnostics, []);
    let program = &analysis.program;
    let node = |name: &str| {
        let output = program.outputs.iter().find(|o| o.name == name);
        &program.nodes[output.unwrap().node]
    };
    let unit = |name: &str| {
        node(name)
            .ty
            .channels
            .get(0)
            .unwrap()
            .unit
            .symbol()
            .to_owned()
    };
    assert_eq!(unit("scaled"), "V");
    // Volts times nanotesla per volt is nanotesla, by algebra.
    assert_eq!(unit("field"), "nT");
    assert_eq!(unit("flux"), "V*s");
    assert_eq!(unit("slope"), "V/s");
    assert_eq!(unit("ratio"), "");

    // A conversion is a gain: a thousand millivolts to the volt.
    assert_eq!(unit("small"), "mV");
    let Op::Scale { factor, divide } = &node("small").op else {
        panic!("a conversion scales");
    };
    assert_eq!((program.value(&factor.expr), *divide), (1000.0, false));
}

#[test]
fn durations_and_frequencies_are_converted_to_base_units() {
    let source = "\
source a = /0/field @rate(48 kHz) @channels(x)
param period : ms = 20
out y = a | lowpass(1.5kHz) | rms(250 ms) | notch(1 / period) | highpass(1 / 2s)
";
    let analysis = lang::analyze(source);
    assert_eq!(analysis.diagnostics, []);
    let program = &analysis.program;
    assert_eq!(program.domains[0].rate, 48_000.0);
    let value = |function: &str| {
        program
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::Call {
                    function: f, args, ..
                } if *f == function => match &args[0].value {
                    ArgValue::Scalar(scalar) => Some(program.value(&scalar.expr)),
                    _ => None,
                },
                _ => None,
            })
            .unwrap()
    };
    assert_eq!(value("lowpass"), 1500.0);
    assert_eq!(value("rms"), 0.25);
    // One over a time is a frequency: 1 / 20 ms is 50 Hz.
    assert_eq!(value("notch"), 50.0);
    assert_eq!(value("highpass"), 0.5);
}

#[test]
fn the_graph_is_drawn_as_checked_in() {
    let analysis = lang::analyze(&example());
    let dot = lang::dot::dot(&analysis.program, "gradiometer");
    let snapshot = crate_file("tests/snapshots/gradiometer.dot");
    if std::env::var_os("DSP_BLESS").is_some() {
        fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        fs::write(&snapshot, &dot).unwrap();
    }
    assert_eq!(
        fs::read_to_string(&snapshot).ok().as_deref(),
        Some(dot.as_str()),
        "run with DSP_BLESS=1 to accept"
    );
    // The tree is not consulted for the drawing; a node count is enough to
    // tie the two together.
    let nodes = dot
        .lines()
        .filter(|l| l.contains("[label=") && !l.contains("->"));
    assert_eq!(
        nodes.count(),
        analysis.program.nodes.len() + analysis.program.outputs.len()
    );
    assert!(analysis
        .syntax
        .children
        .iter()
        .any(|c| matches!(c, SyntaxElement::Node(_))));
}

/// Checks and draws specs through the command line, in this process. The
/// files are named by their full paths, which the comparisons take back out.
#[cfg(feature = "cli")]
#[test]
fn the_command_reports_through_its_exit_code() {
    use common::command;
    let examples = crate_file("examples");
    let diagnostics = crate_file("tests/diagnostics");
    let file =
        |directory: &std::path::Path, name: &str| directory.join(name).to_str().unwrap().to_owned();
    // What the command wrote, with each file named as it is inside
    // `directory`.
    let within = |directory: &std::path::Path, text: &str| {
        let prefix = format!("{}{}", directory.display(), std::path::MAIN_SEPARATOR);
        text.replace(&prefix, "")
    };

    let example = file(&examples, "gradiometer.tsl");
    let (code, stdout, stderr) = command(&["check", &example]);
    assert_eq!((code, stderr.as_str()), (0, ""));
    let stdout = within(&examples, &stdout);
    assert!(stdout.starts_with("gradiometer.tsl: ok\n"));
    assert!(stdout.contains("out    gradient   /255/gradient  3 ch (x, y, z), nT, 500 Hz"));

    let (code, stdout, _) = command(&["graph", &example, "--dot"]);
    assert_eq!(code, 0);
    assert!(stdout.starts_with("digraph \"gradiometer\" {"));
    let (code, stdout, _) = command(&["graph", &example, "--typst"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("#import \"@preview/autograph"), "{stdout}");

    // Errors: the rendered diagnostics, a count, and a failing status.
    let unaligned = file(&diagnostics, "align_rates.tsl");
    let (code, stdout, stderr) = command(&["check", &unaligned]);
    assert_eq!((code, stdout.as_str()), (1, ""));
    let stderr = within(&diagnostics, &stderr);
    let rendered = fs::read_to_string(diagnostics.join("align_rates.stderr")).unwrap();
    assert!(stderr.starts_with(&rendered), "{stderr}");
    assert!(stderr.ends_with("error: align_rates.tsl does not check: 2 errors\n"));
    let (code, stdout, _) = command(&["graph", &unaligned, "--dot"]);
    assert_eq!((code, stdout.as_str()), (1, ""));

    // Warnings alone do not fail.
    let (code, stdout, stderr) = command(&["check", &file(&diagnostics, "unused.tsl")]);
    assert_eq!(code, 0);
    assert!(within(&diagnostics, &stdout).starts_with("unused.tsl: ok\n"));
    assert!(within(&diagnostics, &stderr).ends_with("warning: unused.tsl: 3 warnings\n"));

    // The functions a spec may call are listed as they are written.
    let (code, stdout, _) = command(&["functions"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("  decimate(n)\n"), "{stdout}");

    // A wrong command line or a missing file is a different failure.
    assert_eq!(command(&["check", &file(&examples, "absent.tsl")]).0, 2);
    assert_eq!(command(&["frobnicate"]).0, 2);
    assert_eq!(command(&["serve", &example]).0, 2);
    assert_eq!(command(&["graph", &example]).0, 2);
    assert_eq!(command(&["graph", &example, "--dot", "--typst"]).0, 2);
    assert_eq!(
        command(&["graph", &example, "--dot", "--packages", "."]).0,
        2
    );
    // A mount that names no sensor, or two at one place, is too.
    let (code, _, stderr) = command(&["serve", &example, "--mount", "=/0"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("names no sensor"), "{stderr}");
    let (code, _, stderr) = command(&[
        "serve",
        &example,
        "--mount",
        "tcp://a=/3",
        "--mount",
        "tcp://b=/3",
    ]);
    assert_eq!(code, 2);
    assert!(stderr.contains("two sensors are mounted at /3"), "{stderr}");

    // Help is not a failure, and names the command as it is started.
    let (code, stdout, _) = command(&["--help"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("Usage: tio dsp <COMMAND>"), "{stdout}");
}

/// A program's nodes without where they were written: what each computes,
/// from what, and of what type.
fn shape(program: &lang::Program) -> Vec<(Op, Vec<usize>, twinleaf_dsp::SignalType)> {
    program
        .nodes
        .iter()
        .map(|node| (node.op.clone(), node.inputs.clone(), node.ty.clone()))
        .collect()
}

#[test]
fn a_function_is_its_body_written_out() {
    const HEAD: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)
source b = /1/field @rate(500) @channels(x, y, z) @unit(nT)
param line : Hz = 60 [45 .. 65]
param gain = 2 [0.5 .. 4]
";
    // Written once, and used piped, called, by name, and from another
    // function.
    let folded = format!(
        "{HEAD}
fn clean(s) = s | notch(line, q = 30) | bandpass(1 Hz, 40 Hz)
fn scaled(s, k) = clean(s) * k

out fast : nT = a | clean()
out slow : nT = scaled(k = gain, s = b)
out both : nT = align(a | decimate(2) | clean(), b | clean()) | magnitude()
"
    );
    let unfolded = format!(
        "{HEAD}
out fast : nT = a | notch(line, q = 30) | bandpass(1 Hz, 40 Hz)
out slow : nT = (b | notch(line, q = 30) | bandpass(1 Hz, 40 Hz)) * gain
out both : nT = align(a | decimate(2) | notch(line, q = 30) | bandpass(1 Hz, 40 Hz), b | notch(line, q = 30) | bandpass(1 Hz, 40 Hz)) | magnitude()
"
    );
    let (folded, unfolded) = (lang::analyze(&folded), lang::analyze(&unfolded));
    assert!(folded.diagnostics.is_empty(), "{:?}", folded.diagnostics);
    assert!(
        unfolded.diagnostics.is_empty(),
        "{:?}",
        unfolded.diagnostics
    );
    // Nothing of the functions is left in the program but what their
    // bodies made: the same nodes, wired the same way, of the same types.
    assert_eq!(shape(&folded.program), shape(&unfolded.program));
    assert_eq!(folded.program.params, unfolded.program.params);
    let names = |program: &lang::Program| -> Vec<String> {
        program.outputs.iter().map(|o| o.name.clone()).collect()
    };
    assert_eq!(names(&folded.program), names(&unfolded.program));
}

#[test]
fn a_function_sees_what_it_is_given_and_what_was_defined_above_it() {
    let check = |body: &str| {
        let source = format!(
            "source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)\n\
             source b = /1/field @rate(1000) @channels(x, y, z) @unit(nT)\n\
             param k = 2\n{body}"
        );
        let analysis = lang::analyze(&source);
        let codes: Vec<&str> = analysis.diagnostics.iter().filter_map(|d| d.code).collect();
        (analysis, codes)
    };

    // What it is given hides a definition of the same name: here `a` is
    // whatever the call hands over, which is `b`.
    let (analysis, codes) = check("fn twice(a) = a * k\nout y = twice(b)\n");
    assert_eq!(codes, ["W0001"], "{:?}", analysis.diagnostics);
    assert_eq!(analysis.diagnostics[0].message, "`a` is never used");
    let scale = analysis.program.nodes.last().unwrap();
    assert!(matches!(scale.op, Op::Scale { .. }));
    assert!(matches!(
        analysis.program.nodes[scale.inputs[0]].op,
        Op::Source { source: 1 }
    ));

    // A number is given as readily as a signal, and a use inside a body
    // is a use: neither `k` nor `b` is reported as unused.
    let (analysis, codes) =
        check("fn mix(s, by) = (align(s, b) | magnitude()) * by * k\nout y = a | mix(3)\n");
    assert!(codes.is_empty(), "{:?}", analysis.diagnostics);

    // What was defined after the function is not there for its body, even
    // though it is by the time of the call.
    let (analysis, codes) = check("fn late(s) = s * after\nparam after = 3\nout y = late(a)\n");
    // (The two sources this leaves unused are remarked on first.)
    assert_eq!(codes, ["W0001", "W0001", "E0204"]);
    let error = &analysis.diagnostics[2];
    assert_eq!(error.message, "`after` is used before it is defined");
    assert!(error
        .labels
        .iter()
        .any(|(_, label)| label == "in this call of `late`"));

    // A function is a name like any other: defined once, before its use,
    // and a keyword now, so nothing else can be called `fn`.
    let (_, codes) = check("let fn = a\nout y = a\n");
    assert!(codes.contains(&"E0002"), "{codes:?}");
}

#[test]
fn a_function_that_is_not_called_is_not_checked_and_says_so() {
    // There is nothing to check a body against until a call fills in its
    // names, so a body that could never check goes unreported, and the
    // function is reported instead as what it is: unused.
    let source = "\
source a = /0/field @rate(1000) @channels(x) @unit(nT)
fn never(s) = s | nothing_at_all()
out y = a
";
    let analysis = lang::analyze(source);
    let said: Vec<(&str, &str)> = analysis
        .diagnostics
        .iter()
        .map(|d| (d.code.unwrap_or(""), d.message.as_str()))
        .collect();
    assert_eq!(said, [("W0001", "`never` is never used")]);
}

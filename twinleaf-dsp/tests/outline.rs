//! What the editor is told about a spec, and that every edit it is offered
//! is a splice that leaves a spec.

use std::fs;
use std::path::PathBuf;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::lang::builtins::BUILTINS;
use twinleaf_dsp::lang::outline::{outline, Outline, Range, Shape};
use twinleaf_dsp::lang::{self};
use twinleaf_dsp::lower::{lower, LowerError, SourceFacts, RUNNABLE};
use twinleaf_dsp::Options;

fn example() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/gradiometer.tsl");
    fs::read_to_string(path).unwrap()
}

/// The text a range covers, counted as a text view counts it.
fn text(source: &str, range: Range) -> String {
    let units: Vec<u16> = source.encode_utf16().collect();
    String::from_utf16(&units[range.start..range.end]).unwrap()
}

/// `source` with `range` replaced, as a text view would do it.
fn splice(source: &str, range: Range, with: &str) -> String {
    let units: Vec<u16> = source.encode_utf16().collect();
    let mut out = String::from_utf16(&units[..range.start]).unwrap();
    out.push_str(with);
    out.push_str(&String::from_utf16(&units[range.end..]).unwrap());
    out
}

fn insert(source: &str, at: usize, with: &str) -> String {
    splice(source, Range { start: at, end: at }, with)
}

fn shape<'a>(outline: &'a Outline, title: &str) -> &'a Shape {
    outline
        .nodes
        .iter()
        .find(|shape| shape.title == title)
        .unwrap_or_else(|| panic!("no node called {title}"))
}

fn codes(outline: &Outline) -> Vec<&'static str> {
    outline.problems.iter().filter_map(|p| p.code).collect()
}

#[test]
fn every_token_is_marked_as_what_it_is() {
    let source = example();
    let outline = outline(&source);
    assert!(outline.ok);

    let marked: Vec<(String, &str)> = outline
        .marks
        .iter()
        .map(|mark| (text(&source, mark.range), mark.class))
        .collect();
    let has = |text: &str, class: &str| marked.iter().any(|(t, c)| t == text && *c == class);
    assert!(has("source", "keyword"));
    assert!(has("a", "definition"));
    assert!(has("/0/field", "path"));
    assert!(has("@rate", "attribute"));
    assert!(has("1000", "number"));
    assert!(has("param", "keyword"));
    assert!(has("line", "definition"));
    assert!(has("Hz", "unit"));
    assert!(has("..", "operator"));
    assert!(has("align", "function"));
    assert!(has("decimate", "function"));
    assert!(has("|", "operator"));
    // A param where it is used, apart from any other name.
    assert!(has("line", "param"));
    assert!(has("fr", "name"));
    assert!(has("q", "argument"));
    assert!(has(".a", "channel"));
    assert!(has("nT", "unit"));
    assert!(marked
        .iter()
        .any(|(t, c)| t.starts_with("# One domain") && *c == "comment"));

    // The marks are in order, and what they leave out is only space.
    let units: Vec<u16> = source.encode_utf16().collect();
    let mut at = 0;
    for mark in &outline.marks {
        assert!(mark.range.start >= at && mark.range.end > mark.range.start);
        let between = String::from_utf16(&units[at..mark.range.start]).unwrap();
        assert!(between.trim().is_empty(), "nothing marks {between:?}");
        at = mark.range.end;
    }
    assert!(String::from_utf16(&units[at..]).unwrap().trim().is_empty());
}

#[test]
fn a_number_and_its_unit_are_marked_apart() {
    let source = "param window : s = 10s [1 .. 100]\n";
    let outline = outline(source);
    let marked: Vec<(String, &str)> = outline
        .marks
        .iter()
        .map(|mark| (text(source, mark.range), mark.class))
        .collect();
    assert!(marked.contains(&("10".to_owned(), "number")));
    assert!(marked.contains(&("s".to_owned(), "unit")));
}

#[test]
fn the_graph_is_the_one_the_text_describes() {
    let source = example();
    let outline = outline(&source);
    let titles: Vec<&str> = outline.nodes.iter().map(|s| s.title.as_str()).collect();
    assert_eq!(
        titles,
        [
            "/0/field",
            "/1/field",
            "decimate",
            "align",
            "notch",
            "bandpass",
            ".a",
            ".b",
            "−",
            "/255/gradient"
        ]
    );

    let id = |title: &str| shape(&outline, title).id.clone();
    assert_eq!(shape(&outline, "decimate").inputs, [id("/0/field")]);
    assert_eq!(
        shape(&outline, "align").inputs,
        [id("decimate"), id("/1/field")]
    );
    assert_eq!(shape(&outline, "notch").inputs, [id("align")]);
    assert_eq!(shape(&outline, "bandpass").inputs, [id("notch")]);
    assert_eq!(shape(&outline, "−").inputs, [id(".a"), id(".b")]);
    assert_eq!(shape(&outline, "/255/gradient").inputs, [id("−")]);

    // What each is given, what the spec calls it, and what it carries.
    assert_eq!(shape(&outline, "decimate").detail, "2");
    assert_eq!(shape(&outline, "notch").detail, "line, q = 30");
    assert_eq!(shape(&outline, "bandpass").detail, "lo, hi, order = 4");
    assert_eq!(shape(&outline, "/0/field").name.as_deref(), Some("a"));
    assert_eq!(shape(&outline, "align").name.as_deref(), Some("fr"));
    assert_eq!(shape(&outline, "bandpass").name.as_deref(), Some("clean"));
    assert_eq!(shape(&outline, "notch").name, None);
    assert_eq!(shape(&outline, "/0/field").signal, "1 kHz · 3 ch · nT");
    assert_eq!(shape(&outline, "align").signal, "500 Hz · 6 ch · nT");
    assert_eq!(
        shape(&outline, "/255/gradient").signal,
        "500 Hz · 3 ch · nT"
    );
    assert_eq!(
        shape(&outline, "align").channels,
        ["a.x", "a.y", "a.z", "b.x", "b.y", "b.z"]
    );

    // Where each is written: all that makes it, and the part that is its own.
    let written = |title: &str| {
        let shape = shape(&outline, title);
        (text(&source, shape.range), text(&source, shape.focus))
    };
    assert_eq!(
        written("notch"),
        (
            "fr | notch(line, q = 30)".to_owned(),
            "notch(line, q = 30)".to_owned()
        )
    );
    assert_eq!(
        written("decimate"),
        ("a | decimate(2)".to_owned(), "decimate(2)".to_owned())
    );
    assert_eq!(written("align").1, "align(a | decimate(2), b)");
    assert_eq!(
        written("−"),
        ("clean.a - clean.b".to_owned(), "-".to_owned())
    );
    assert_eq!(written(".a").1, "clean.a");
    assert!(written("/0/field").1.starts_with("source a = /0/field"));
}

#[test]
fn taking_a_stage_out_deletes_its_text_and_nothing_else() {
    let source = example();
    let before = outline(&source);

    // Only what is piped can be taken out.
    let removable: Vec<&str> = before
        .nodes
        .iter()
        .filter(|shape| shape.remove.is_some())
        .map(|shape| shape.title.as_str())
        .collect();
    assert_eq!(removable, ["decimate", "notch", "bandpass"]);

    // Without the notch the rest stands, and `line` is left unused.
    let without = splice(&source, shape(&before, "notch").remove.unwrap(), "");
    assert!(without.contains("let clean = fr | bandpass(lo, hi, order = 4)\n"));
    assert_eq!(without.len(), source.len() - " | notch(line, q = 30)".len());
    let after = outline(&without);
    assert!(after.ok);
    assert_eq!(codes(&after), ["W0001"]);

    // Without the decimator the arms no longer share a rate, and the text
    // says so itself.
    let without = splice(&source, shape(&before, "decimate").remove.unwrap(), "");
    assert!(without.contains("let fr = align(a, b)\n"));
    assert_eq!(codes(&outline(&without)), ["E0412"]);
}

#[test]
fn a_stage_is_offered_only_where_a_pipe_can_go() {
    let source = example();
    let outline = outline(&source);
    let offered: Vec<&str> = outline
        .nodes
        .iter()
        .filter(|shape| shape.append.is_some())
        .map(|shape| shape.title.as_str())
        .collect();
    // Not after an operand of the subtraction, which would need
    // parentheses, and not on a source, which is used by name.
    assert_eq!(
        offered,
        [
            "decimate",
            "align",
            "notch",
            "bandpass",
            "−",
            "/255/gradient"
        ]
    );

    // Wherever one is offered, piping it on leaves text that parses.
    for shape in &outline.nodes {
        let Some(at) = shape.append else {
            assert!(shape.stages.is_empty());
            continue;
        };
        assert!(!shape.stages.is_empty());
        for stage in &shape.stages {
            let edited = insert(&source, at, &stage.text);
            let problems = codes(&self::outline(&edited));
            assert!(
                !problems.iter().any(|code| code.starts_with("E00")),
                "{} after {} gives {problems:?}",
                stage.text,
                shape.title
            );
        }
    }

    // After the notch, the stage lands between it and the band-pass.
    let notch = shape(&outline, "notch");
    let edited = insert(&source, notch.append.unwrap(), " | lowpass(50 Hz)");
    assert!(edited.contains("fr | notch(line, q = 30) | lowpass(50 Hz) | bandpass(lo, hi"));
    assert!(self::outline(&edited).ok);
}

#[test]
fn every_stage_offered_is_written_to_suit_the_signal() {
    // On an output, where nothing downstream can object, each stage that
    // is offered checks as it is written, whatever the rate.
    for rate in [25.0, 500.0, 1000.0, 48_000.0] {
        let source = format!(
            "source a = /0/field @rate({rate}) @channels(x, y, z) @unit(nT)\n\nout y = a\n"
        );
        let before = outline(&source);
        let output = shape(&before, "/255/y");
        let functions: Vec<&str> = output.stages.iter().map(|s| s.function).collect();
        assert_eq!(
            functions,
            [
                "decimate",
                "resample",
                "notch",
                "bandpass",
                "lowpass",
                "highpass",
                "magnitude",
                "rms",
                "integrate",
                "derivative"
            ]
        );
        for stage in &output.stages {
            let edited = insert(&source, output.append.unwrap(), &stage.text);
            let after = outline(&edited);
            assert!(
                after.ok,
                "{} at {rate} Hz: {:?}",
                stage.text,
                after
                    .problems
                    .iter()
                    .map(|p| &p.message)
                    .collect::<Vec<_>>()
            );
            // The new stage is a node, fed by the source and feeding the
            // output.
            let made = shape(&after, stage.function);
            assert_eq!(made.inputs, [shape(&after, "/0/field").id.clone()]);
            assert_eq!(
                shape(&after, "/255/y").inputs,
                std::slice::from_ref(&made.id)
            );
        }
    }

    // Nothing follows a conversion, so nothing is offered after one.
    let source = "source a = /0/field @rate(100) @channels(x) @unit(nT)\nout y = a in pT\n";
    let outline = outline(source);
    assert!(outline.ok);
    assert!(outline.nodes.iter().all(|shape| shape.append.is_none()));
}

#[test]
fn a_param_is_set_by_replacing_its_digits() {
    let source = example();
    let before = outline(&source);
    let names: Vec<&str> = before.params.iter().map(|k| k.name.as_str()).collect();
    assert_eq!(names, ["line", "lo", "hi"]);
    let line = &before.params[0];
    assert_eq!(
        (line.value, line.low, line.high),
        (60.0, Some(45.0), Some(65.0))
    );
    assert_eq!(line.unit, "Hz");
    assert_eq!(text(&source, line.digits), "60");
    assert!(text(&source, line.range).starts_with("param line : Hz = 60"));

    let edited = splice(&source, line.digits, "50.5");
    assert_eq!(edited.len(), source.len() + 2);
    let after = outline(&edited);
    assert!(after.ok);
    assert_eq!(after.params[0].value, 50.5);
    // The others are where they were, two characters on.
    assert_eq!(
        after.params[1].digits.start,
        before.params[1].digits.start + 2
    );

    // A param without a unit says so by having none.
    let plain = outline("param gain = 2 [0.1 .. 10]\n");
    assert_eq!(plain.params[0].unit, "");

    // A value written with its unit keeps the unit.
    let source = "param window : s = -10s [-20 .. 100]\n";
    let knob = &outline(source).params[0];
    assert_eq!(text(source, knob.digits), "-10");
    assert_eq!(knob.value, -10.0);
}

#[test]
fn a_problem_says_where_it_is_as_a_text_view_counts() {
    // The `µ` is two bytes and one character: what follows it is a byte
    // further on than a text view thinks.
    let source = "\
source a = /0/field @rate(1000) @channels(x) @unit(µT)
out y = ab | notch(60)
";
    let outline = outline(source);
    assert!(!outline.ok);
    // One error, and beside it the source that is now never used.
    let errors: Vec<_> = outline
        .problems
        .iter()
        .filter(|p| p.severity == "error")
        .collect();
    assert_eq!(errors.len(), 1);
    let problem = errors[0];
    assert_eq!(problem.code, Some("E0202"));
    assert_eq!(problem.message, "cannot find `ab`");
    assert_eq!(text(source, problem.range), "ab");
    assert_eq!((problem.line, problem.column), (2, 9));
    assert_eq!(problem.notes.len(), 1);
    assert_eq!(problem.notes[0].kind, "help");
    assert_eq!(problem.notes[0].text, "did you mean `a`?");
    assert!(problem
        .rendered
        .starts_with("error[E0202]: cannot find `ab`"));
    assert!(problem.rendered.contains("--> dsp.spec:2:9"));

    // What could be read is still drawn: the source is there.
    assert_eq!(outline.nodes[0].title, "/0/field");
    assert_eq!(outline.nodes[0].signal, "1 kHz · 1 ch · µT");
}

#[test]
fn a_function_not_known_here_is_drawn_and_remarked_on() {
    // An editor reads text bound for a graph that may have functions of
    // its own. It cannot check a call of one, and must not refuse it.
    let source = "\
source a = /0/field @rate(1000) @channels(x, y) @unit(nT)
param width = 5 [3 .. 99]
out y = a | despike(width, mode = fast) | lowpass(40 Hz)
";
    let drawn = outline(source);
    assert!(drawn.ok, "{:?}", drawn.problems);
    assert_eq!(drawn.problems.len(), 1);
    let remark = &drawn.problems[0];
    assert_eq!((remark.severity, remark.code), ("note", None));
    assert_eq!(remark.message, "`despike` is not a function known here");
    assert_eq!(text(source, remark.range), "despike");
    // The param it is given counts as used, and there is nothing to
    // suggest in place of a name that is close to none.
    assert!(remark.notes.iter().all(|note| note.kind == "note"));

    // It is a node like another, passing its input's signal on, and what
    // follows it is typed from that.
    let node = shape(&drawn, "despike");
    assert_eq!(node.kind, "function");
    assert!(node.runs);
    assert_eq!(node.inputs, [shape(&drawn, "/0/field").id.clone()]);
    assert_eq!(node.signal, "1 kHz · 2 ch · nT");
    assert_eq!(
        shape(&drawn, "lowpass").inputs,
        std::slice::from_ref(&node.id)
    );
    assert_eq!(text(source, node.focus), "despike(width, mode = fast)");
    // It can be taken out and built on like any stage.
    assert!(node.remove.is_some() && node.append.is_some());

    // A name one slip from a function that is known is still let through,
    // with the function it may have been meant for.
    let slip = outline(&source.replace("lowpass", "lowpas"));
    assert!(slip.ok);
    let help: Vec<&str> = slip
        .problems
        .iter()
        .flat_map(|p| &p.notes)
        .filter(|note| note.kind == "help")
        .map(|note| note.text.as_str())
        .collect();
    assert_eq!(help, ["did you mean `lowpass`?"]);

    // Read strictly, as the graph that runs it reads it, the same text
    // does not check.
    assert!(lang::analyze(source).has_errors());

    // And one given no signal to pass on is no better here than there.
    let orphan = outline("param width = 5\nout y = despike(width)\n");
    assert!(!orphan.ok);
    assert!(codes(&orphan).contains(&"E0203"));
}

#[test]
fn a_problem_with_two_places_names_both() {
    let source = "\
source a = /0/field @rate(1000) @channels(x) @unit(nT)
source b = /1/field @rate(500) @channels(x) @unit(nT)
out y = align(a, b)
";
    let outline = outline(source);
    let problem = &outline.problems[0];
    assert_eq!(problem.code, Some("E0412"));
    let places: Vec<String> = std::iter::once(problem.range)
        .chain(problem.related.iter().map(|r| r.range))
        .map(|range| text(source, range))
        .collect();
    assert!(places.contains(&"a".to_owned()) && places.contains(&"b".to_owned()));
}

#[test]
fn text_that_is_no_spec_at_all_still_has_an_outline() {
    for source in ["", "   \n", "let", "out y = ", "🙂 source", "param = = ="] {
        let outline = outline(source);
        let units = source.encode_utf16().count();
        for mark in &outline.marks {
            assert!(mark.range.start < mark.range.end && mark.range.end <= units);
        }
        for problem in &outline.problems {
            assert!(problem.range.start <= problem.range.end && problem.range.end <= units);
        }
    }
}

/// A valid call of every function, to tell the ones that run from the ones
/// that only check.
const CALLS: [(&str, &str); 14] = [
    ("align", "align(a, b)"),
    ("decimate", "a | decimate(2)"),
    ("resample", "a | resample(500 Hz)"),
    ("notch", "a | notch(60 Hz)"),
    ("bandpass", "a | bandpass(1 Hz, 40 Hz)"),
    ("lowpass", "a | lowpass(10 Hz)"),
    ("highpass", "a | highpass(1 Hz)"),
    ("detrend", "a | detrend(mean)"),
    ("magnitude", "a | magnitude()"),
    // Its reference must share its domain, so both come from one `align`.
    (
        "decorrelate",
        "fr.a | decorrelate(ref = fr.b, window = 10 s)",
    ),
    ("psd", "a | psd(1 s)"),
    ("rms", "a | rms(1 s)"),
    ("integrate", "a | integrate()"),
    ("derivative", "a | derivative()"),
];

#[test]
fn the_functions_said_to_run_are_the_ones_that_do() {
    let called: Vec<&str> = CALLS.iter().map(|(name, _)| *name).collect();
    let known: Vec<&str> = BUILTINS.iter().map(|builtin| builtin.name).collect();
    assert_eq!(called, known);

    for (name, call) in CALLS {
        let source = format!(
            "source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)\n\
             source b = /1/field @rate(1000) @channels(x, y, z) @unit(nT)\n\
             let fr = align(a, b)\n\
             out y = {call}\n\
             out other = fr\n"
        );
        let analysis = lang::analyze(&source);
        assert!(!analysis.has_errors(), "{call} does not check");
        let facts = SourceFacts {
            epoch: Epoch::UNIX,
            timebase: SessionId::new(1),
            origin: 0.0,
        };
        let program = &analysis.program;
        let built = lower(
            program,
            &[facts; 2],
            &program.declared(),
            Options::default(),
        );
        let runs = !matches!(built, Err(LowerError::Unsupported { .. }));
        assert_eq!(runs, RUNNABLE.contains(&name), "{name}");
        if runs {
            assert!(built.is_ok(), "{call} does not build");
        }

        // And the node the editor draws for it says the same.
        let drawn = outline(&source);
        assert!(drawn
            .nodes
            .iter()
            .any(|s| s.title == name && s.runs == runs));
        assert!(drawn.nodes.iter().all(|s| s.runs || s.title == name));
    }
}

#[test]
fn a_function_is_coloured_as_one_and_its_calls_are_drawn_as_their_bodies() {
    let source = "\
source a = /0/field @rate(1000) @channels(x, y) @unit(nT)
source b = /1/field @rate(1000) @channels(x, y) @unit(nT)

fn clean(s) = s | notch(60 Hz) | lowpass(40 Hz)

out fast = a | clean()
out slow = clean(b)
";
    let drawn = outline(source);
    assert!(
        drawn.ok && drawn.problems.is_empty(),
        "{:?}",
        drawn.problems
    );

    let marked: Vec<(String, &str)> = drawn
        .marks
        .iter()
        .map(|mark| (text(source, mark.range), mark.class))
        .collect();
    let has = |text: &str, class: &str| marked.iter().any(|(t, c)| t == text && *c == class);
    assert!(has("fn", "keyword"));
    assert!(has("clean", "definition") && has("s", "definition"));
    assert!(has("clean", "function"));

    // Each call is its body's nodes: two notches and two low-passes, one
    // pair on each source, and nothing called `clean`.
    let titles: Vec<&str> = drawn.nodes.iter().map(|s| s.title.as_str()).collect();
    assert_eq!(
        titles,
        [
            "/0/field",
            "/1/field",
            "notch",
            "lowpass",
            "notch",
            "lowpass",
            "/255/fast",
            "/255/slow"
        ]
    );
    let notches: Vec<&Shape> = drawn.nodes.iter().filter(|s| s.title == "notch").collect();
    assert_eq!(notches[0].inputs, [shape(&drawn, "/0/field").id.clone()]);
    assert_eq!(notches[1].inputs, [shape(&drawn, "/1/field").id.clone()]);

    // Both are written in the one place, the function's body, so an edit
    // made at either is an edit of the function, and so of both.
    assert_eq!(notches[0].focus, notches[1].focus);
    assert_eq!(text(source, notches[0].focus), "notch(60 Hz)");
    let lowpass = shape(&drawn, "lowpass");
    let at = lowpass.append.expect("a stage can follow the body");
    let edited = insert(source, at, " | highpass(1 Hz)");
    assert!(edited.contains("fn clean(s) = s | notch(60 Hz) | lowpass(40 Hz) | highpass(1 Hz)\n"));
    let after = outline(&edited);
    assert!(after.ok);
    assert_eq!(
        after.nodes.iter().filter(|s| s.title == "highpass").count(),
        2
    );
}

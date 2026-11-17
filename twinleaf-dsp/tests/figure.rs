//! The figure of a spec: what is written for Typst, and, with the `typst`
//! feature, that Typst makes a figure of it.

use std::fs;
use std::path::PathBuf;
use twinleaf_dsp::lang::{self, typst};

fn crate_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn example() -> String {
    fs::read_to_string(crate_file("examples/gradiometer.tsl")).unwrap()
}

/// A spec whose names and units are full of what Typst reads as markup:
/// `_` opens emphasis, `*` strong text, `/` and `^` mean something in
/// math, and `#` begins code.
const AWKWARD: &str = "\
source a_1 = /0/field.raw @rate(1000) @channels(x_1, y_1) @unit(nT/V)
param gain_a : V = 2 [0.5 .. 4]

let scaled = a_1 * gain_a   # in nT
out total_i = scaled | integrate()
out size : nT = scaled | magnitude()
";

/// The text of `source` outside its string literals.
fn outside_strings(source: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    let mut escaped = false;
    for character in source.chars() {
        match (inside, escaped, character) {
            (true, true, _) => escaped = false,
            (true, false, '\\') => escaped = true,
            (true, false, '"') => inside = false,
            (true, false, _) => {}
            (false, _, '"') => inside = true,
            (false, _, other) => out.push(other),
        }
    }
    assert!(!inside, "a string was left open");
    out
}

#[test]
fn the_figure_is_written_as_checked_in() {
    let analysis = lang::analyze(&example());
    let figure = typst::figure(&analysis.program, "gradiometer.tsl");
    let snapshot = crate_file("tests/snapshots/gradiometer.typ");
    if std::env::var_os("DSP_BLESS").is_some() {
        fs::write(&snapshot, &figure).unwrap();
    }
    assert_eq!(
        fs::read_to_string(&snapshot).ok().as_deref(),
        Some(figure.as_str()),
        "run with DSP_BLESS=1 to accept"
    );
}

#[test]
fn the_figure_is_written_as_autograph_reads_it() {
    let analysis = lang::analyze(&example());
    let program = &analysis.program;
    let figure = typst::figure(program, "gradiometer.tsl");

    // The versions it was written against, and no others.
    assert!(figure.contains("#import \"@preview/autograph:0.1.0\": diagram, node, edge"));
    assert!(figure.contains("#import \"@preview/fletcher:0.5.8\": shapes"));

    // A node for every node of the program and for every output, each
    // named by a label and given its content.
    let nodes: Vec<&str> = figure
        .lines()
        .filter(|line| line.trim_start().starts_with("node(<"))
        .collect();
    assert_eq!(nodes.len(), program.nodes.len() + program.outputs.len());

    // An edge for every input, naming two nodes that exist. Autograph
    // takes nothing else in an edge's place but named arguments, and
    // refuses a label there.
    let edges: Vec<&str> = figure
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("edge("))
        .collect();
    let inputs: usize = program.nodes.iter().map(|node| node.inputs.len()).sum();
    assert_eq!(edges.len(), inputs + program.outputs.len());
    for edge in edges {
        let inner = edge
            .strip_prefix("edge(")
            .and_then(|rest| rest.strip_suffix("),"))
            .unwrap();
        let parts: Vec<&str> = inner.split(", ").collect();
        assert_eq!(parts.len(), 3, "{edge}");
        for end in &parts[..2] {
            assert!(nodes
                .iter()
                .any(|node| node.contains(&format!("node({end},"))));
        }
        assert_eq!(parts[2], "..wire");
    }

    // A signal is described where it is made, and not again where nothing
    // about it changes: the notch passes on what `align` gave it.
    let line = |what: &str| nodes.iter().find(|node| node.contains(what)).unwrap();
    assert!(line("\"align\"").contains("\"500 Hz · 6 ch · nT\""));
    assert!(!line("\"notch(line, q = 30)\"").contains("Hz ·"));
    assert!(line("\"/255/gradient\"").contains("\"500 Hz · 3 ch · nT\""));

    // It sets nothing about the page, so that it can be included. The
    // document made of it for compiling does.
    assert!(!figure.contains("#set"));
    let document = typst::document(program, "gradiometer.tsl");
    assert!(document.starts_with("#set page(width: auto, height: auto"));
    assert!(document.ends_with(&figure));
}

#[test]
fn nothing_from_the_spec_is_left_for_typst_to_read_as_markup() {
    let analysis = lang::analyze(AWKWARD);
    assert!(!analysis.has_errors());
    let figure = typst::figure(&analysis.program, "awkward_name #1.tsl");

    // What the spec says is all there, in strings.
    for said in [
        "\"/0/field.raw\"",
        "\"× gain_a\"",
        "\"integrate\"",
        "\"/255/total_i\"",
        "nT/V",
    ] {
        assert!(figure.contains(said), "{said} is missing from\n{figure}");
    }
    // And outside the strings, past the comment at the top, there is none
    // of it: no name, no unit, nothing but the figure's own words.
    let body: String = figure.lines().skip(1).collect::<Vec<_>>().join("\n");
    let bare = outside_strings(&body);
    for word in [
        "gain_a",
        "a_1",
        "total_i",
        "nT",
        "field",
        "x_1",
        "integrate",
    ] {
        assert!(!bare.contains(word), "{word} is bare in\n{bare}");
    }
    assert!(!bare.contains('*') && !bare.contains('_') && !bare.contains('$'));
}

#[cfg(feature = "typst")]
mod compiled {
    use super::*;
    use twinleaf_dsp::figure::{render, Format};

    // These need the packages the figure imports. The first run fetches
    // them from Typst Universe into the user's package cache, so they are
    // not run unless asked for: `cargo test --features typst -- --ignored`.

    #[test]
    #[ignore = "fetches the figure's Typst packages on first use"]
    fn typst_makes_a_pdf_and_an_svg_of_the_figure() {
        let analysis = lang::analyze(&example());
        let document = typst::document(&analysis.program, "gradiometer.tsl");
        let pdf = render(&document, Format::Pdf, None).unwrap();
        assert!(pdf.starts_with(b"%PDF-"));
        let svg = String::from_utf8(render(&document, Format::Svg, None).unwrap()).unwrap();
        assert!(svg.starts_with("<svg"));
        // Text is drawn as glyphs, so the words are not there to find. A
        // shape for every node is.
        assert!(svg.matches("<path").count() > analysis.program.nodes.len());
    }

    #[test]
    #[ignore = "fetches the figure's Typst packages on first use"]
    fn names_and_units_full_of_markup_compile_as_written() {
        let analysis = lang::analyze(AWKWARD);
        let document = typst::document(&analysis.program, "awkward_name #1.tsl");
        assert!(render(&document, Format::Pdf, None)
            .unwrap()
            .starts_with(b"%PDF-"));
    }

    #[test]
    fn what_typst_objects_to_is_reported_with_its_line() {
        // No package is needed to get this wrong.
        let error =
            render("#set page(width: auto)\n\n#nonsense()\n", Format::Pdf, None).unwrap_err();
        let said = error.to_string();
        assert!(
            said.contains("line 3: unknown variable: nonsense"),
            "{said}"
        );
    }
}

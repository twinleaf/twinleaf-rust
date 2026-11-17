//! Changing a spec while it runs: every change is a change to its text.

use twinleaf_dsp::run::RunOptions;
use twinleaf_dsp::session::{Session, SetError};

const SPEC: &str = "\
# A filter with three knobs.
source a = /0/field @rate(500) @channels(x, y, z) @unit(nT)

param line : Hz = 60        [45 .. 65]   # the mains
param window = 10s
param edge : Hz = 40

out clean = a | notch(line) | lowpass(edge) | highpass(1 / window)
";

fn open() -> Session {
    Session::open(SPEC.to_owned(), RunOptions::default(), "the device").unwrap()
}

#[test]
fn setting_a_param_rewrites_its_literal_and_nothing_else() {
    let mut session = open();
    assert_eq!(session.revision(), 1);
    session.set_param("line", 55.0).unwrap();
    assert_eq!(session.source(), SPEC.replace("= 60 ", "= 55 "));
    assert_eq!(session.params()[0].value, 55.0);
    assert_eq!(session.revision(), 2);

    // A unit written on the number stays on it.
    session.set_param("window", 2.5).unwrap();
    assert!(session.source().contains("param window = 2.5s\n"));
    assert_eq!(session.params()[1].value, 2.5);
    assert_eq!(session.params()[1].unit.symbol(), "s");

    // Setting a param to what it already is changes nothing.
    session.set_param("line", 55.0).unwrap();
    assert_eq!(session.revision(), 3);
}

#[test]
fn a_value_the_spec_cannot_take_is_refused_and_changes_nothing() {
    let mut session = open();
    assert_eq!(
        session.set_param("line", 70.0),
        Err(SetError::OutOfRange {
            name: "line".into(),
            value: 70.0,
            low: 45.0,
            high: 65.0,
        })
    );
    assert_eq!(
        session.set_param("gain", 2.0),
        Err(SetError::Unknown("gain".into()))
    );
    assert!(matches!(
        session.set_param("edge", f64::NAN),
        Err(SetError::NotANumber { .. })
    ));
    // `edge` has no range of its own, but the signal it filters has a
    // Nyquist frequency, and the checker holds it to that.
    let refused = session.set_param("edge", 300.0).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "`edge` cannot be 300: `f` of `lowpass` reaches the Nyquist frequency of its signal"
    );
    assert_eq!(session.source(), SPEC);
    assert_eq!(session.revision(), 1);
    // The message of a range error is the one a run gives for `--set`.
    assert_eq!(
        session.set_param("line", 70.0).unwrap_err().to_string(),
        "`line` cannot be 70: its range is 45 to 65"
    );
}

#[test]
fn a_spec_that_does_not_check_is_not_taken() {
    let mut session = open();
    let broken = SPEC.replace("lowpass(edge)", "lowpas(edge)");
    let diagnostics = session.replace(broken).unwrap_err();
    assert_eq!(diagnostics[0].code, Some("E0203"));
    assert_eq!(session.source(), SPEC);

    let wider = SPEC.replace("[45 .. 65]", "[40 .. 70]");
    session.replace(wider.clone()).unwrap();
    assert_eq!(session.source(), wider);
    session.set_param("line", 70.0).unwrap();
    assert_eq!(session.waiting_for(), ["/0/field"]);
    assert!(!session.is_running());
}

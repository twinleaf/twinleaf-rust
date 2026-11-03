//! How a run takes the generated cases and reports them, and the variables
//! of the environment that steer it.

use super::classes::CLASSES;
use super::driver::{check, Checked, Run};
use super::laws::{Broken, OPEN};
use super::schedule::{drawn, runner, Case};
use proptest::test_runner::TestCaseError;
use std::collections::BTreeMap;
use std::time::Duration;

/// How many cases a run takes unless `TWINLEAF_TURMOIL_CASES` says.
const CASES: u64 = 24;
/// How many times a failing case is shrunk unless
/// `TWINLEAF_TURMOIL_SHRINKS` says.
const SHRINKS: u32 = 64;

/// Prints the log of `run`.
pub(super) fn dump(case: &Case, run: &Run) {
    eprintln!(
        "{case:?}\nwarmed {} healed {} settled {} end {}",
        run.warmed, run.healed, run.settled, run.end
    );
    for logged in &run.log {
        eprintln!("{:>7} {:?}", logged.at, logged.event);
    }
}

/// The message of a case that broke `broken`.
fn message(case: &Case, broken: &Broken) -> String {
    format!(
        "{} broke at {} ms in the case of seed {}: {}\nrun it alone with TWINLEAF_TURMOIL_CASE={}",
        broken.law, broken.at, case.seed, broken.what, case.seed
    )
}

/// What the cases of one run counted.
#[derive(Default)]
pub(super) struct Totals {
    cases: u64,
    real: Duration,
    slowest: Duration,
    pub(super) classes: BTreeMap<&'static str, u64>,
    open: BTreeMap<&'static str, Vec<String>>,
    pub(super) unjudged: Vec<String>,
}

impl Totals {
    fn add(&mut self, case: &Case, checked: &Checked) {
        self.cases += 1;
        self.real += checked.real;
        self.slowest = self.slowest.max(checked.real);
        for class in &checked.classes {
            *self.classes.entry(class).or_default() += 1;
        }
        for broken in checked
            .broken
            .iter()
            .filter(|broken| OPEN.contains(&broken.law))
        {
            self.open
                .entry(broken.law)
                .or_default()
                .push(message(case, broken));
        }
        if let Some(why) = &checked.unjudged {
            self.unjudged.push(format!("seed {}: {why}", case.seed));
        }
    }

    pub(super) fn print(&self) {
        let cases = self.cases.max(1);
        eprintln!(
            "{} cases in {:?}, {:?} each on average, the slowest {:?}",
            self.cases,
            self.real,
            self.real / cases as u32,
            self.slowest
        );
        for class in CLASSES {
            let count = self.classes.get(class).copied().unwrap_or(0);
            eprintln!("  {count:>5}  {class}");
        }
        for law in OPEN {
            let found = self.open.get(law).map_or(&[][..], Vec::as_slice);
            eprintln!("  {:>5}  times the open law {law} broke", found.len());
            for message in found.iter().take(3) {
                eprintln!("         {}", message.replace('\n', "\n         "));
            }
        }
        for why in &self.unjudged {
            eprintln!("  unjudged {why}");
        }
    }
}

/// The seeds of the cases a run takes, those `TWINLEAF_TURMOIL_CASE` lists
/// with commas between when it is set, and otherwise 1 to
/// `TWINLEAF_TURMOIL_CASES`, or to [`CASES`].
pub(super) fn case_seeds() -> Vec<u64> {
    if let Ok(seeds) = std::env::var("TWINLEAF_TURMOIL_CASE") {
        return seeds
            .split(',')
            .map(|seed| seed.trim().parse().expect("a seed"))
            .collect();
    }
    let cases = std::env::var("TWINLEAF_TURMOIL_CASES")
        .map_or(CASES, |cases| cases.parse().expect("a count"));
    (1..=cases).collect()
}

/// How many times a failing case is shrunk.
fn shrinks() -> u32 {
    std::env::var("TWINLEAF_TURMOIL_SHRINKS")
        .map_or(SHRINKS, |shrinks| shrinks.parse().expect("a count"))
}

/// How many cases run at once, each on a thread of its own with its own
/// simulation, as `TWINLEAF_TURMOIL_THREADS` says or one per core.
pub(super) fn threads() -> usize {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    std::env::var("TWINLEAF_TURMOIL_THREADS")
        .map_or(cores, |threads| threads.parse().expect("a count"))
}

/// Runs the case of `seed`, shrinking it if it fails. Adds what its first
/// run counted to `totals`, and returns the failure of the smallest case.
pub(super) fn exercise(seed: u64, totals: &std::sync::Mutex<Totals>) -> Option<String> {
    let first = std::cell::Cell::new(true);
    let outcome = runner(seed, shrinks()).run(&drawn(), |(swarm, schedule)| {
        let case = Case {
            seed,
            swarm,
            schedule,
        };
        let checked = check(&case);
        if first.replace(false) {
            crate::chan::lock(totals).add(&case, &checked);
        }
        if let Some(why) = &checked.unjudged {
            let rerun = format!("run it alone with TWINLEAF_TURMOIL_CASE={seed}");
            return Err(TestCaseError::fail(format!(
                "harness error in the case of seed {seed}: {why}\n{rerun}"
            )));
        }
        match checked
            .broken
            .iter()
            .find(|broken| !OPEN.contains(&broken.law))
        {
            Some(broken) => Err(TestCaseError::fail(message(&case, broken))),
            None => Ok(()),
        }
    });
    outcome.err().map(|failure| failure.to_string())
}

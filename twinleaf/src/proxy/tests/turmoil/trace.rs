//! A record of turmoil's own events, which tells whether a run made twice
//! with one seed ran the same way.

/// One event of turmoil's, as a trace keeps it.
#[derive(Debug, PartialEq)]
pub(super) struct Traced {
    /// How many steps of the simulation had finished when the step it
    /// happened in began, which is its virtual time in milliseconds.
    step: u64,
    /// The host it happened on, or `sim` for the simulation's own.
    host: String,
    /// The event's message and its fields, as turmoil gave them.
    what: String,
}

thread_local! {
    /// What this thread records while [`traced`] runs a scenario on it.
    static TRACE: std::cell::RefCell<Option<Trace>> = const { std::cell::RefCell::new(None) };
}

/// The events one thread records, and what it needs to place them.
#[derive(Default)]
struct Trace {
    events: Vec<Traced>,
    /// The name of each span, whose id is its place here plus one. A host's
    /// span on each step reuses the id of its first.
    spans: Vec<String>,
    /// The ids of the spans entered, innermost last.
    entered: Vec<u64>,
    /// How many steps had finished when the one that runs now began.
    step: u64,
}

/// Returns the events turmoil makes on this thread while `scenario` runs,
/// in order.
pub(super) fn traced(scenario: impl FnOnce()) -> Vec<Traced> {
    Tracer::install();
    TRACE.with(|trace| *trace.borrow_mut() = Some(Trace::default()));
    scenario();
    let trace = TRACE.with(|trace| trace.borrow_mut().take());
    trace.expect("the thread's trace").events
}

/// The process's `tracing` subscriber, which hands each of turmoil's events
/// to the [`Trace`] of the thread it happens on, if that thread records one.
/// Turmoil marks the start of each step with an event, and runs each host
/// inside a span that names it.
///
/// It is the global default rather than a thread's own, as `tracing` asks
/// each new callsite of the default of the thread that reaches it first.
/// A scenario on another thread would then turn off turmoil's callsites
/// for every thread.
pub(super) struct Tracer;

impl Tracer {
    /// Makes the tracer the process's subscriber, once. Every scenario
    /// installs it before turmoil runs, so turmoil's callsites are on from
    /// the start.
    pub(super) fn install() {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            ::tracing::subscriber::set_global_default(Tracer).expect("no other subscriber");
            // A callsite reached while the tracer was not yet the default
            // took the interest of none.
            ::tracing::callsite::rebuild_interest_cache();
        });
    }
}

/// The fields of one event or span, written out in order.
#[derive(Default)]
struct Fields {
    message: String,
    name: String,
    rest: String,
}

impl ::tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &::tracing::field::Field, value: &str) {
        match field.name() {
            "name" => self.name = value.to_string(),
            _ => self.record_debug(field, &value),
        }
    }

    fn record_debug(&mut self, field: &::tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            name => {
                let _ = write!(self.rest, " {name}={value:?}");
            }
        }
    }
}

/// Runs `f` on this thread's trace, if it records one.
fn with_trace(f: impl FnOnce(&mut Trace)) {
    TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            f(trace);
        }
    });
}

impl ::tracing::Subscriber for Tracer {
    fn enabled(&self, metadata: &::tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with("turmoil")
    }

    fn new_span(&self, span: &::tracing::span::Attributes<'_>) -> ::tracing::span::Id {
        let mut fields = Fields::default();
        span.record(&mut fields);
        let mut id = 1;
        with_trace(|trace| {
            let spans = &mut trace.spans;
            let at = match spans.iter().position(|name| *name == fields.name) {
                Some(at) => at,
                None => {
                    spans.push(fields.name);
                    spans.len() - 1
                }
            };
            id = at as u64 + 1;
        });
        ::tracing::span::Id::from_u64(id)
    }

    fn record(&self, _: &::tracing::span::Id, _: &::tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &::tracing::span::Id, _: &::tracing::span::Id) {}

    fn event(&self, event: &::tracing::Event<'_>) {
        with_trace(|trace| {
            let mut fields = Fields::default();
            event.record(&mut fields);
            if let Some(step) = fields.message.strip_prefix("step ") {
                trace.step = step.parse().expect("a step number");
                return;
            }
            let host = match trace.entered.last() {
                Some(&id) => trace.spans[id as usize - 1].clone(),
                None => "sim".to_string(),
            };
            trace.events.push(Traced {
                step: trace.step,
                host,
                what: fields.message + &fields.rest,
            });
        });
    }

    fn enter(&self, span: &::tracing::span::Id) {
        with_trace(|trace| trace.entered.push(span.into_u64()));
    }

    fn exit(&self, _: &::tracing::span::Id) {
        with_trace(|trace| {
            trace.entered.pop();
        });
    }
}

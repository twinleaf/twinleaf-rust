//! Law 12 of the outbox's spec under shuttle's schedulers. A thread makes
//! one call while the writer queues and takes on another. The takes and the
//! drain after them must be what the model accepts with that call made whole
//! at one place among the writer's calls, and each race must reach every
//! place. A failure prints the schedule that broke it, which replays as
//! `crate::chan` says.

use super::super::{Taken, TAKE_LIMIT};
use super::{drain, Classes, Client, Item, Kind, Model, Queue, CLASSES};
use crate::chan;
use crate::proto::DeviceRoute;
use crate::tio::packet::Packet;
use ::shuttle::thread;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// How many schedules each scheduler runs per property.
const ITERATIONS: usize = 1000;

/// The id of the watch whose update races the takes.
const ID: u16 = 1;

/// One call on the outbox, as the model makes it.
enum Call {
    /// A call that queues these items on this queue.
    Queue(Vec<Item>, Queue),
    /// The take numbered so among the writer's takes.
    Take(usize),
    /// The proxy stops.
    Stop,
}

/// Makes `call` on `model`, where `takes` are what the writer's takes
/// brought.
fn make(
    model: &mut Model,
    call: &Call,
    takes: &[Taken],
    classes: &mut Classes,
) -> Result<(), String> {
    match call {
        Call::Queue(items, queue) => model.queue(items, *queue),
        Call::Take(n) => model.take(&takes[*n], classes)?,
        Call::Stop => model.stop(),
    }
    Ok(())
}

/// Returns how many of the `writer`'s calls the model makes before
/// `racing` so that it accepts `takes` and then `drain`, or what broke at
/// each place.
fn linearize(
    model: &Model,
    writer: &[Call],
    racing: &Call,
    takes: &[Taken],
    drain: &[Taken],
) -> Result<usize, String> {
    let mut broken = Vec::new();
    for at in 0..=writer.len() {
        let mut model = model.clone();
        let mut classes = Classes::default();
        let mut run = || {
            for call in &writer[..at] {
                make(&mut model, call, takes, &mut classes)?;
            }
            make(&mut model, racing, takes, &mut classes)?;
            for call in &writer[at..] {
                make(&mut model, call, takes, &mut classes)?;
            }
            for taken in drain {
                model.take(taken, &mut classes)?;
            }
            model.drained()
        };
        match run() {
            Ok(()) => return Ok(at),
            Err(law) => broken.push(format!("with the call after {at} of the writer's, {law}")),
        }
    }
    Err(broken.join("\n"))
}

/// Runs `scenario`, which returns the place [`linearize`] found, under
/// shuttle's schedulers. Checks that every one of the `places` was reached,
/// unless one schedule is replayed. `name` names the race in what it prints.
fn race<const PLACES: usize>(
    name: &'static str,
    scenario: impl Fn() -> Result<usize, String> + Send + Sync + 'static,
) {
    let places = Arc::new([const { AtomicUsize::new(0) }; PLACES]);
    chan::check(ITERATIONS, {
        let places = Arc::clone(&places);
        move || {
            match scenario() {
            Ok(at) => {
                places[at].fetch_add(1, Ordering::Relaxed);
            }
            Err(broken) => panic!(
                "law 12, no place of the call among the writer's gives what the writer took\n{broken}"
            ),
        }
        }
    });
    let reached = places.each_ref().map(|n| n.load(Ordering::Relaxed));
    if std::env::var_os(CLASSES).is_some() {
        eprintln!(
            "{name}, how often the call came after each number of the writer's calls {reached:?}"
        );
    }
    if std::env::var_os("TWINLEAF_SHUTTLE_SCHEDULE").is_none() {
        assert!(
            !reached.contains(&0),
            "law 12, the {name} race never reached some places {reached:?}"
        );
    }
}

/// Races an update of `count` parts, metadata if `metadata`, with two takes,
/// once `fill` queued what the outbox holds first. `fill` is given the
/// model's items for the parts.
fn update_race(
    metadata: bool,
    count: usize,
    fill: fn(&mut Client, &[Item]),
) -> Result<usize, String> {
    let mut client = Client::default();
    let (update, items, queue) = client.prepare(ID, metadata, count);
    fill(&mut client, &items);
    let racer = thread::spawn({
        let outbox = Arc::clone(&client.outbox);
        move || outbox.update(ID, &update)
    });
    let takes = [client.outbox.take(), client.outbox.take()];
    racer.join().expect("the update's thread");
    client.outbox.check();
    let drain = drain(&client.outbox);
    let writer = [Call::Take(0), Call::Take(1)];
    let racing = Call::Queue(items, queue);
    linearize(&client.model, &writer, &racing, &takes, &drain)
}

/// Queues an answer and two samples.
fn a_little(client: &mut Client, _: &[Item]) {
    client.answer(8);
    client.sample(16);
    client.sample(16);
}

/// Queues samples that leave room under [`TAKE_LIMIT`] for the first of
/// `parts` and not for the second, so a take that brings the first part
/// reaches the limit inside the update.
fn room_for_one_part(client: &mut Client, parts: &[Item]) {
    let (first, second) = (parts[0].bytes, parts[1].bytes);
    let most = TAKE_LIMIT - first;
    let least = most - second + 1;
    let packet = Packet::samples(1, 0, 0, &[0; 4], DeviceRoute::root());
    let overhead = packet.expect("samples that fit").as_bytes().len() - 4;
    let mut share = 0;
    while share < least {
        share += client.sample((most - share - overhead).min(480));
    }
    assert!(share <= most, "the samples left no room for the first part");
}

/// An urgent update queued while the writer takes twice reaches it whole,
/// behind what the urgent queue held, at one of the takes or at the drain.
#[test]
fn an_urgent_update_racing_the_takes_goes_whole_before_between_or_after_them() {
    race::<3>("urgent update", || update_race(false, 3, a_little));
}

/// A metadata update queued while the writer takes twice reaches it whole,
/// behind the stream it was queued after. The take that brings its first
/// part reaches the limit inside it and brings the rest too.
#[test]
fn a_metadata_update_racing_the_takes_goes_whole_before_between_or_after_them() {
    race::<3>("metadata update", || {
        update_race(true, 3, room_for_one_part)
    });
}

/// The proxy stops while the writer queues an answer, takes, queues a sample
/// and takes. What was queued before the stop goes out ahead of the status
/// that the proxy stopped, what was queued after it is refused, and nothing
/// goes after the status.
#[test]
fn a_stop_racing_the_writer_refuses_what_comes_after_it_and_goes_last() {
    race::<5>("stop", || {
        let mut client = Client::default();
        client.answer(8);
        client.sample(16);
        let answer = client.answer_packet(8);
        let sample = client.sample_packet(16);
        let writer = [
            Call::Queue(vec![Item::new(&answer, Kind::Other)], Queue::Urgent),
            Call::Take(0),
            Call::Queue(vec![Item::new(&sample, Kind::Sample)], Queue::Stream),
            Call::Take(1),
        ];
        let racer = thread::spawn({
            let outbox = Arc::clone(&client.outbox);
            move || outbox.stop()
        });
        client.outbox.urgent(answer);
        let first = client.outbox.take();
        client.outbox.stream(sample);
        let takes = [first, client.outbox.take()];
        racer.join().expect("the stop's thread");
        client.outbox.check();
        let drain = drain(&client.outbox);
        linearize(&client.model, &writer, &Call::Stop, &takes, &drain)
    });
}

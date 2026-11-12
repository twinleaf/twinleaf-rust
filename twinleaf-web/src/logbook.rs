//! The log every page shows, which the hub keeps as lines.
//!
//! A line is said by a device or by the proxy. A device's line is a log
//! packet it sent, which [`read`] takes from the hub's connection as it
//! takes every packet of it. The proxy's lines are the hub's own, of what
//! it sees of its devices. A device found, a table that did not load, and
//! each loss of data, which `loss` counts and says here as it counts it.
//! A line of a loss carries what was lost as data, a [`LossLine`], so no
//! page reads it out of the words.
//!
//! The proxy's lines are also what the process that hosts the proxy writes
//! on its console. Its logger hands each record it prints to [`Console`],
//! which gives it to the log of the hub there is, as a line of no device
//! whose words are the record's source and text as the console prints
//! them. With no hub yet, or between two, the console keeps the newest
//! [`HELD`] records and the next hub's log takes them, each with the time
//! it was said, so the lines of the proxy's start are in the log. What the
//! log's own owners write on the console of a line they also say here is
//! written inside [`unheard`], so it is in the log once.
//!
//! Every line has two times. `wall` is the host's clock as the hub took
//! the line, in Unix seconds. `time` is on the clock the plots use, the
//! time of the newest sample the hub had taken of the line's device as it
//! took the line. A device's log packet carries no time the hub can place,
//! so the line is no later than that sample by more than what the hub's
//! queues held and one sample period. A device no sample was taken of has
//! no `time`, and neither has a line of no device.
//!
//! Each line goes to every page as a change, numbered from 1 in the order
//! the hub took them, and the hub keeps the newest [`KEPT`] for a page's
//! snapshot, so a page that opens later sees what led up to now. A line may
//! reach a page both in a snapshot and as a change after it. Its number
//! tells the page it holds it already.

use crate::feed::Feed;
use crate::protocol::{Frame, Level, LossLine, Message, Source};
use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use twinleaf::proto::log::LogLevel;
use twinleaf::tio::packet::Payload;
use twinleaf::tio::Packet;
use twinleaf::{DeviceRoute, Receiver, RecvError};

/// How many of the newest lines the hub keeps for a page's snapshot.
pub(crate) const KEPT: usize = 500;

/// How many of the newest records of the console wait for a hub's log.
pub(crate) const HELD: usize = 200;

/// The host's clock now, in Unix seconds.
fn wall() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |since| since.as_secs_f64())
}

/// The lines kept, and the feed each goes out on.
pub(crate) struct Logbook {
    state: Mutex<State>,
    feed: Arc<Feed>,
}

#[derive(Default)]
struct State {
    /// The number of the last line taken.
    last: u64,
    /// The newest lines, oldest first, at most [`KEPT`].
    lines: VecDeque<Message>,
    /// The time of the newest sample taken of each device.
    newest: BTreeMap<DeviceRoute, f64>,
}

impl Logbook {
    pub(crate) fn new(feed: Arc<Feed>) -> Arc<Logbook> {
        Arc::new(Logbook {
            state: Mutex::new(State::default()),
            feed,
        })
    }

    /// Notes `time` as that of the newest sample taken of the device at
    /// `route`, which the lines of that device taken next carry.
    pub(crate) fn heard(&self, route: DeviceRoute, time: f64) {
        self.state.lock().unwrap().newest.insert(route, time);
    }

    /// Takes a line the proxy says at `level`, of the device at `route` or
    /// of none, with what it says was lost, if it is of a loss.
    pub(crate) fn proxy(
        &self,
        level: Level,
        route: Option<DeviceRoute>,
        text: String,
        loss: Option<LossLine>,
    ) {
        self.line(Source::Proxy, level, route, text, loss, wall());
    }

    /// Takes a line the device at `route` said at `level`.
    pub(crate) fn device(&self, route: DeviceRoute, level: Level, text: String) {
        self.line(Source::Device, level, Some(route), text, None, wall());
    }

    /// Numbers a line said at the host's time `wall`, stamps it, sends it
    /// and keeps it. The lock is held until it is sent, so the lines go out
    /// in the order of their numbers.
    fn line(
        &self,
        source: Source,
        level: Level,
        route: Option<DeviceRoute>,
        text: String,
        loss: Option<LossLine>,
        wall: f64,
    ) {
        let mut state = self.state.lock().unwrap();
        state.last += 1;
        let message = Message::Log {
            n: state.last,
            wall: (wall * 1000.0).round() / 1000.0,
            time: route.and_then(|route| state.newest.get(&route).copied()),
            level,
            source,
            route: route.map(|route| route.to_string()),
            text,
            loss,
        };
        self.feed.message(&message);
        if state.lines.len() == KEPT {
            state.lines.pop_front();
        }
        state.lines.push_back(message);
    }

    /// The lines kept, oldest first, for a page's snapshot.
    pub(crate) fn snapshot(&self) -> Vec<Frame> {
        let state = self.state.lock().unwrap();
        state.lines.iter().map(Message::frame).collect()
    }
}

/// The records the hosting process writes on its console, on their way to
/// the log of the hub there is.
pub(crate) struct Console {
    state: Mutex<Waiting>,
}

/// The log the records go to, while its hub lives, and those that wait for
/// one, oldest first, each with its level, its words and when it was said.
struct Waiting {
    log: Weak<Logbook>,
    held: VecDeque<(Level, String, f64)>,
}

/// The console of this process.
pub(crate) static CONSOLE: Console = Console::new();

thread_local! {
    /// Whether this thread is inside [`unheard`] or inside the console's
    /// own taking of a record.
    static UNHEARD: Cell<bool> = const { Cell::new(false) };
}

/// Runs `said`, which writes on the console what the log already holds a
/// line of. No console takes a record this thread logs meanwhile.
pub(crate) fn unheard<T>(said: impl FnOnce() -> T) -> T {
    let was = UNHEARD.replace(true);
    let out = said();
    UNHEARD.set(was);
    out
}

impl Console {
    pub(crate) const fn new() -> Console {
        Console {
            state: Mutex::new(Waiting {
                log: Weak::new(),
                held: VecDeque::new(),
            }),
        }
    }

    /// Takes a record the console printed at `level`, from `source` as the
    /// console names it, saying `text`. It is a line of the hub's log at
    /// once, or waits for a hub, the oldest of more than [`HELD`] going. A
    /// record logged inside [`unheard`], or by what this sets off, is not
    /// taken.
    pub(crate) fn take(&self, level: log::Level, source: &str, text: &str) {
        if UNHEARD.get() {
            return;
        }
        let level = match level {
            log::Level::Error => Level::Error,
            log::Level::Warn => Level::Warn,
            log::Level::Info => Level::Info,
            log::Level::Debug | log::Level::Trace => Level::Debug,
        };
        let text = format!("{source}: {text}");
        unheard(|| {
            let mut state = self.state.lock().unwrap();
            match state.log.upgrade() {
                Some(log) => log.line(Source::Proxy, level, None, text, None, wall()),
                None => {
                    if state.held.len() == HELD {
                        state.held.pop_front();
                    }
                    state.held.push_back((level, text, wall()));
                }
            }
        });
    }

    /// Makes `log` the log the records go to, for as long as its hub
    /// lives, and gives it those that waited, in the order they were said.
    pub(crate) fn attach(&self, log: &Arc<Logbook>) {
        unheard(|| {
            let mut state = self.state.lock().unwrap();
            state.log = Arc::downgrade(log);
            for (level, text, wall) in std::mem::take(&mut state.held) {
                log.line(Source::Proxy, level, None, text, None, wall);
            }
        });
    }
}

/// The level of a line a device sent at `level`. A critical line is an
/// error, and a level the protocol does not name is information.
pub(crate) fn level_of(level: LogLevel) -> Level {
    match level {
        LogLevel::CRITICAL | LogLevel::ERROR => Level::Error,
        LogLevel::WARNING => Level::Warn,
        LogLevel::DEBUG => Level::Debug,
        _ => Level::Info,
    }
}

/// Takes each log packet of `packets`, every packet of the hub's
/// connection, as a line of the device that sent it, until the connection
/// ends. Packets the queue dropped as this fell behind may have held
/// lines, and a line of the proxy's says how many packets they were.
pub(crate) async fn read(packets: Receiver<Packet>, log: Arc<Logbook>) {
    loop {
        match packets.recv_async().await {
            Ok(packet) => {
                if let Payload::Log(message) = packet.payload() {
                    let text = String::from_utf8_lossy(message.message).into_owned();
                    log.device(packet.route(), level_of(message.level), text);
                }
            }
            Err(RecvError::Lagged(missed)) => log.proxy(
                Level::Warn,
                None,
                format!("The log fell behind {missed} packets, and lines among them are missing"),
                None,
            ),
            Err(_) => return,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tokio::sync::broadcast;

    /// The `log` messages `rx` heard, as JSON.
    pub(crate) fn heard(rx: &mut broadcast::Receiver<Arc<Frame>>) -> Vec<serde_json::Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str::<serde_json::Value>(text).ok(),
                Frame::Binary(_) => None,
            })
            .filter(|message| message["type"] == "log")
            .collect()
    }

    /// A line goes out as a change, numbered one more than the line before,
    /// with who said it, of which device, its level, its words and what it
    /// says was lost. Its `time` is the newest sample time noted of its
    /// device before it, and none for a device not heard or for no device.
    #[test]
    fn a_line_is_numbered_and_carries_the_newest_sample_time_of_its_device() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let log = Logbook::new(feed);
        let root = DeviceRoute::root();
        log.device(root, Level::Warn, "early".to_string());
        log.heard(root, 12.5);
        log.heard(root, 12.75);
        log.device(root, Level::Debug, "later".to_string());
        log.proxy(Level::Info, None, "of no device".to_string(), None);
        let loss = LossLine::Skipped {
            stream: "field".to_string(),
            samples: 5,
        };
        log.proxy(Level::Warn, Some(root), "5 lost".to_string(), Some(loss));
        let sent = heard(&mut rx);
        let numbers: Vec<u64> = sent.iter().map(|m| m["n"].as_u64().unwrap()).collect();
        assert_eq!(numbers, [1, 2, 3, 4]);
        assert_eq!(sent[0]["time"], serde_json::Value::Null);
        assert_eq!(sent[0]["level"], "warn");
        assert_eq!(sent[0]["source"], "device");
        assert_eq!(sent[0]["route"], "/");
        assert_eq!(sent[0]["text"], "early");
        assert!(sent[0].get("loss").is_none(), "a line of no loss has none");
        assert_eq!(sent[1]["time"], 12.75);
        assert_eq!(sent[1]["level"], "debug");
        assert_eq!(sent[2]["time"], serde_json::Value::Null);
        assert_eq!(sent[2]["route"], serde_json::Value::Null);
        assert_eq!(sent[2]["source"], "proxy");
        assert_eq!(sent[3]["time"], 12.75);
        assert_eq!(
            sent[3]["loss"],
            serde_json::json!({"cause": "skipped", "stream": "field", "samples": 5})
        );
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let wall = sent[3]["wall"].as_f64().unwrap();
        assert!((now.as_secs_f64() - wall).abs() < 60.0, "{wall}");
        assert!(sent.iter().all(|m| m["seq"].as_u64().is_some()));
    }

    /// A snapshot holds the newest KEPT lines, oldest first, and none
    /// older.
    #[test]
    fn a_snapshot_holds_the_newest_lines_up_to_the_bound() {
        let log = Logbook::new(Feed::new());
        assert!(log.snapshot().is_empty());
        for i in 0..KEPT + 7 {
            log.proxy(Level::Info, None, format!("line {i}"), None);
        }
        let kept: Vec<serde_json::Value> = log
            .snapshot()
            .iter()
            .map(|frame| match frame {
                Frame::Text(text) => serde_json::from_str(text).unwrap(),
                Frame::Binary(_) => panic!("a line is a message"),
            })
            .collect();
        assert_eq!(kept.len(), KEPT);
        assert_eq!(kept[0]["n"], 8);
        assert_eq!(kept[0]["text"], "line 7");
        assert_eq!(kept[KEPT - 1]["n"], (KEPT + 7) as u64);
        assert!(kept[0].get("seq").is_none(), "a snapshot's has no number");
    }

    /// A console record said before any hub waits, and is a line of the
    /// first log attached, with the time it was said, its level, and its
    /// source and text as its words. One said after goes to that log at
    /// once. Once that hub is gone the records wait again, for the next.
    #[test]
    fn a_console_record_waits_for_a_hubs_log_and_then_is_a_line_of_it() {
        let console = Console::new();
        let before = wall();
        console.take(log::Level::Warn, "udp://127.0.0.1", "Sensor disconnected");
        console.take(log::Level::Trace, "worker", "Port 3 opened");
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let log = Logbook::new(feed);
        assert!(heard(&mut rx).is_empty(), "no hub has taken them yet");
        std::thread::sleep(std::time::Duration::from_millis(20));
        console.attach(&log);
        console.take(log::Level::Error, "server", "client 3 ended");
        let said = heard(&mut rx);
        let read = |line: &serde_json::Value| {
            let text = line["text"].as_str().unwrap().to_string();
            (line["level"].as_str().unwrap().to_string(), text)
        };
        assert_eq!(
            said.iter().map(read).collect::<Vec<_>>(),
            [
                (
                    "warn".to_string(),
                    "udp://127.0.0.1: Sensor disconnected".to_string()
                ),
                ("debug".to_string(), "worker: Port 3 opened".to_string()),
                ("error".to_string(), "server: client 3 ended".to_string()),
            ]
        );
        for line in &said {
            assert_eq!(line["source"], "proxy");
            assert_eq!(line["route"], serde_json::Value::Null);
            assert_eq!(line["time"], serde_json::Value::Null);
        }
        let waited = said[0]["wall"].as_f64().unwrap();
        assert!(waited >= before - 0.001 && waited < said[2]["wall"].as_f64().unwrap() - 0.01);
        assert_eq!(log.snapshot().len(), 3, "each is in the log once");

        drop(log);
        console.take(log::Level::Info, "server", "between two hubs");
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let next = Logbook::new(feed);
        console.attach(&next);
        let said = heard(&mut rx);
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0]["text"], "server: between two hubs");
        assert_eq!(said[0]["n"], 1);
    }

    /// The console keeps the newest HELD records that wait, and no older.
    #[test]
    fn the_console_keeps_the_newest_records_that_wait_up_to_its_bound() {
        let console = Console::new();
        for i in 0..HELD + 5 {
            console.take(log::Level::Info, "server", &format!("record {i}"));
        }
        let log = Logbook::new(Feed::new());
        console.attach(&log);
        let kept = log.snapshot();
        assert_eq!(kept.len(), HELD);
        let Frame::Text(first) = &kept[0] else {
            panic!("a line is a message")
        };
        assert!(first.contains("server: record 5\""), "{first}");
    }

    /// A record logged inside `unheard` is not taken, on this thread, and
    /// one after it is.
    #[test]
    fn a_record_the_logs_owner_writes_of_its_own_line_is_not_taken() {
        let console = Console::new();
        let log = Logbook::new(Feed::new());
        console.attach(&log);
        unheard(|| console.take(log::Level::Warn, "loss", "said in the log already"));
        assert!(log.snapshot().is_empty());
        console.take(log::Level::Warn, "loss", "said on the console alone");
        assert_eq!(log.snapshot().len(), 1);
    }

    /// A device's critical line reads as an error, and a level the protocol
    /// does not name as information.
    #[test]
    fn a_devices_level_maps_onto_the_four_the_page_shows() {
        assert_eq!(level_of(LogLevel::CRITICAL), Level::Error);
        assert_eq!(level_of(LogLevel::ERROR), Level::Error);
        assert_eq!(level_of(LogLevel::WARNING), Level::Warn);
        assert_eq!(level_of(LogLevel::INFO), Level::Info);
        assert_eq!(level_of(LogLevel::DEBUG), Level::Debug);
        assert_eq!(level_of(LogLevel::new(9)), Level::Info);
    }
}

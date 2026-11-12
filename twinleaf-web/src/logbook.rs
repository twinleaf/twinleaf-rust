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
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use twinleaf::proto::log::LogLevel;
use twinleaf::tio::packet::Payload;
use twinleaf::tio::Packet;
use twinleaf::{DeviceRoute, Receiver, RecvError};

/// How many of the newest lines the hub keeps for a page's snapshot.
pub(crate) const KEPT: usize = 500;

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
        self.line(Source::Proxy, level, route, text, loss);
    }

    /// Takes a line the device at `route` said at `level`.
    pub(crate) fn device(&self, route: DeviceRoute, level: Level, text: String) {
        self.line(Source::Device, level, Some(route), text, None);
    }

    /// Numbers a line, stamps it, sends it and keeps it. The lock is held
    /// until it is sent, so the lines go out in the order of their numbers.
    fn line(
        &self,
        source: Source,
        level: Level,
        route: Option<DeviceRoute>,
        text: String,
        loss: Option<LossLine>,
    ) {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |since| since.as_secs_f64());
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

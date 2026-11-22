//! What the hub's connection lost of each device, and where, counted since
//! the hub started. Every page is sent the counts as they grow.
//!
//! A sample can be lost in four places before the hub's history holds it,
//! and [`Lost`] counts the rows of a stream lost in each. The hub's buffer
//! sums them by stream as each batch says what was lost before it, and the
//! counts sent are the buffer's.
//!
//! - `skipped`: the device sent none, or its link lost them while it stayed
//!   up. The sample numbers jump.
//! - `link`: the link to the device was down, or the hub's own connection
//!   to the proxy was.
//! - `proxy`: the proxy dropped them from its queue for the hub's
//!   connection, which read too slowly.
//! - `hub`: the hub's connection took them and dropped them, as the hub's
//!   thread fell behind its queue.
//!
//! A link that drops is counted by device as well, in [`Link`]: how often
//! it dropped and for how long in all. A device that restarts while its
//! link is down sends rows no count of lost rows can be told for, so the
//! drops of the link say what the rows cannot.
//!
//! The fifth place is past the hub: a page that reads too slowly falls
//! behind the hub's changes. That is each page's own, and the snapshot the
//! page is sent in their place says how many (`Message::Hub`).
//!
//! Each loss is a line of the log too, as it is counted. A link that drops
//! is one line, with why the device was lost and the link's address where
//! the hub knows them, and its return another, which says how long it was
//! down.
//! The rows a stream lost since the counts were last sent are a line for
//! each place they were lost, but for those lost with the link down, which
//! the link's own lines stand for.

use crate::feed::Feed;
use crate::logbook::{self, Logbook};
use crate::protocol::{Frame, Level, Link, LossLine, Lost, Message, StreamLoss};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use twinleaf::data::{Buffer, StreamKey};
use twinleaf::device::{LinkState, LostCause};
use twinleaf::DeviceRoute;

/// The counts of every device, and the feed they go out on.
pub(crate) struct Losses {
    devices: Mutex<BTreeMap<DeviceRoute, Device>>,
    /// The name and the rate of each stream that lost rows, as its run
    /// last said them.
    names: Mutex<HashMap<StreamKey, (String, f64)>>,
    feed: Arc<Feed>,
    /// The log, which each loss is a line of.
    log: Arc<Logbook>,
}

/// What was lost of one device.
#[derive(Default)]
struct Device {
    /// How often the link dropped.
    drops: u64,
    /// How long the outages that ended lasted in all, in seconds.
    seconds: f64,
    /// Whether the device was heard since the hub started and its link is
    /// up, and when the link dropped, while it is down.
    up: bool,
    down: Option<Instant>,
    /// What the last message said, or nothing lost before the first.
    sent: (Link, BTreeMap<String, StreamLoss>),
}

impl Device {
    /// How often the link dropped and for how long, up to `now` while it is
    /// down, to a tenth of a second.
    fn link(&self, now: Instant) -> Link {
        let down = self.down.map_or(0.0, |since| {
            now.saturating_duration_since(since).as_secs_f64()
        });
        Link {
            times: self.drops,
            seconds: ((self.seconds + down) * 10.0).round() / 10.0,
        }
    }
}

impl Losses {
    pub(crate) fn new(feed: Arc<Feed>, log: Arc<Logbook>) -> Arc<Losses> {
        Arc::new(Losses {
            devices: Mutex::new(BTreeMap::new()),
            names: Mutex::new(HashMap::new()),
            feed,
            log,
        })
    }

    /// Counts a drop of the link to the device at `route`, which went down
    /// at `since`. Only a link that was up drops, so a device lost before
    /// the hub heard it counts none, and a link that is down drops no
    /// further. `cause` is why the device was lost, if that is known, and
    /// `link` is what the proxy says of the link that serves the device, if
    /// it has. The line of the drop carries both.
    pub(crate) fn link_down(
        &self,
        route: DeviceRoute,
        since: Instant,
        cause: Option<LostCause>,
        link: Option<&LinkState>,
    ) {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(route).or_default();
        if std::mem::take(&mut device.up) {
            device.down = Some(since);
            device.drops += 1;
            let why = cause.map(LostCause::name);
            let url = link.map(|link| link.url.clone());
            let text = match &url {
                Some(url) => format!("The link at {url} is down"),
                None => "The link is down".to_string(),
            };
            let loss = LossLine::Link {
                seconds: None,
                why,
                url,
            };
            self.log.proxy(Level::Error, Some(route), text, Some(loss));
        }
    }

    /// Notes that the device at `route` is heard, which ends the outage of
    /// its link if it is down. `link` is what the proxy says of the link,
    /// whose address the line of the return carries.
    pub(crate) fn link_up(&self, route: DeviceRoute, link: Option<&LinkState>) {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(route).or_default();
        device.up = true;
        if let Some(since) = device.down.take() {
            let down = since.elapsed().as_secs_f64();
            device.seconds += down;
            let seconds = (down * 10.0).round() / 10.0;
            let loss = LossLine::Link {
                seconds: Some(seconds),
                why: None,
                url: link.map(|link| link.url.clone()),
            };
            let text = format!("The link is back after {seconds:.1} s");
            self.log.proxy(Level::Info, Some(route), text, Some(loss));
        }
    }

    /// Sends the counts of each device whose counts changed since they were
    /// last sent, with a line of the log for the rows each stream lost
    /// meanwhile in each place, and logs how many rows the hub dropped. The
    /// rows each stream lost are those `buffer`, the hub's, counted.
    pub(crate) fn send_changed(&self, buffer: &Buffer) {
        let now = Instant::now();
        let mut streams = streams(buffer, &mut self.names.lock().unwrap());
        let mut devices = self.devices.lock().unwrap();
        for route in streams.keys() {
            devices.entry(*route).or_default();
        }
        let mut dropped = 0;
        for (route, device) in devices.iter_mut() {
            let counts = (device.link(now), streams.remove(route).unwrap_or_default());
            if device.sent == counts {
                continue;
            }
            let hub = |streams: &BTreeMap<String, StreamLoss>| -> u64 {
                streams.values().map(|stream| stream.lost.hub).sum()
            };
            dropped += hub(&counts.1) - hub(&device.sent.1);
            for (stream, now) in &counts.1 {
                let before = device.sent.1.get(stream).copied().unwrap_or_default();
                self.lines(*route, stream, &now.lost, &before.lost);
            }
            self.feed.message(&message(*route, &counts));
            device.sent = counts;
        }
        if dropped > 0 {
            // The lines above say it in the log.
            logbook::unheard(|| {
                log::warn!("The web page's hub fell behind and dropped {dropped} samples");
            });
        }
    }

    /// Says in the log what stream `stream` of the device at `route` lost
    /// between the counts `before` and `lost`, a line for each place.
    fn lines(&self, route: DeviceRoute, stream: &str, lost: &Lost, before: &Lost) {
        let skipped = lost.skipped - before.skipped;
        let proxy = lost.proxy - before.proxy;
        let hub = lost.hub - before.hub;
        let stream = || stream.to_string();
        let said = [
            (
                skipped,
                "skipped by the device or lost on its link",
                LossLine::Skipped {
                    stream: stream(),
                    samples: skipped,
                },
            ),
            (
                proxy,
                "dropped by the proxy",
                LossLine::Proxy {
                    stream: stream(),
                    samples: proxy,
                },
            ),
            (
                hub,
                "dropped by the hub",
                LossLine::Hub {
                    stream: stream(),
                    samples: hub,
                },
            ),
        ];
        for (samples, how, loss) in said {
            if samples > 0 {
                let text = format!("{samples} samples of {} {how}", stream());
                self.log.proxy(Level::Warn, Some(route), text, Some(loss));
            }
        }
    }

    /// The counts last sent of each device that lost anything, for a page's
    /// snapshot.
    pub(crate) fn snapshot(&self) -> Vec<Frame> {
        let devices = self.devices.lock().unwrap();
        let lost = devices
            .iter()
            .filter(|(_, device)| device.sent != Default::default());
        lost.map(|(route, device)| message(*route, &device.sent).frame())
            .collect()
    }

    /// The counts last sent of the device at `route`, how often its link
    /// dropped and the rows each of its streams lost, or `None` for a device
    /// not counted.
    pub(crate) fn counts(
        &self,
        route: DeviceRoute,
    ) -> Option<(Link, BTreeMap<String, StreamLoss>)> {
        let devices = self.devices.lock().unwrap();
        devices.get(&route).map(|device| device.sent.clone())
    }

    /// How often the link to the device at `route` dropped.
    #[cfg(test)]
    pub(crate) fn drops(&self, route: DeviceRoute) -> u64 {
        let devices = self.devices.lock().unwrap();
        devices.get(&route).map_or(0, |device| device.drops)
    }
}

/// The rows each stream `buffer` counted lost, by the stream's device and
/// name, for the streams that lost any, those of one name summed, with the
/// stream's rate. `names` keeps the name and the rate each stream's run
/// last said, so a stream that holds no run now goes on under the name it
/// had. One that never held a run has no name yet, and its rows are told
/// once it has one, as the counts only grow.
fn streams(
    buffer: &Buffer,
    names: &mut HashMap<StreamKey, (String, f64)>,
) -> BTreeMap<DeviceRoute, BTreeMap<String, StreamLoss>> {
    let mut streams: BTreeMap<DeviceRoute, BTreeMap<String, StreamLoss>> = BTreeMap::new();
    for (key, lost) in buffer.losses() {
        if let Some(run) = buffer.run(key) {
            let name = run.metadata().schema().stream().name();
            let rate = run.metadata().rate_hz();
            if names
                .get(key)
                .is_none_or(|(known, was)| known != name || *was != rate)
            {
                names.insert(*key, (name.to_string(), rate));
            }
        }
        let Some((name, rate)) = names.get(key) else {
            continue;
        };
        let by_name = streams.entry(key.route).or_default();
        let stream = by_name.entry(name.clone()).or_default();
        stream.rate = *rate;
        stream.lost += Lost::from(lost);
    }
    streams
}

/// The rows `buffer` counted lost of stream `stream` of the device at
/// `route`.
#[cfg(test)]
pub(crate) fn of(buffer: &Buffer, route: DeviceRoute, stream: &str) -> Lost {
    let streams = streams(buffer, &mut HashMap::new());
    let lost = streams.get(&route).and_then(|streams| streams.get(stream));
    lost.map(|stream| stream.lost).unwrap_or_default()
}

/// The message of the `counts` of the device at `route`.
fn message(route: DeviceRoute, (link, streams): &(Link, BTreeMap<String, StreamLoss>)) -> Message {
    Message::Loss {
        route: route.to_string(),
        link: *link,
        streams: streams.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logbook::tests::heard as lines;
    use std::time::Duration;

    /// The counts over `feed`, with a log of their own on it.
    fn losses(feed: Arc<Feed>) -> Arc<Losses> {
        Losses::new(feed.clone(), Logbook::new(feed))
    }

    /// The loss messages `rx` heard, as JSON.
    fn heard(rx: &mut tokio::sync::broadcast::Receiver<Arc<Frame>>) -> Vec<serde_json::Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str::<serde_json::Value>(text).unwrap(),
                Frame::Binary(_) => panic!("a loss is a message"),
            })
            .filter(|message| message["type"] == "loss")
            .collect()
    }

    /// A link that drops is counted once however often it is said to be
    /// down, and its outage lasts from when it went down until it is up. A
    /// message goes out only when the counts changed, and a snapshot holds
    /// the last.
    #[test]
    fn a_link_that_drops_is_counted_once_with_how_long_it_was_down() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let losses = losses(feed);
        let root = DeviceRoute::root();
        losses.send_changed(&Buffer::new(0));
        assert!(heard(&mut rx).is_empty(), "nothing was lost");

        let since = Instant::now() - Duration::from_secs(3);
        losses.link_down(root, since, None, None);
        losses.send_changed(&Buffer::new(0));
        assert!(heard(&mut rx).is_empty(), "a link never up did not drop");
        losses.link_up(root, None);
        losses.link_down(root, since, None, None);
        losses.link_down(root, Instant::now(), None, None);
        losses.link_up(root, None);
        losses.link_up(root, None);
        losses.send_changed(&Buffer::new(0));
        losses.send_changed(&Buffer::new(0));
        let sent = heard(&mut rx);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0]["type"], "loss");
        assert_eq!(sent[0]["route"], "/");
        assert_eq!(sent[0]["link"]["times"], 1);
        let seconds = sent[0]["link"]["seconds"].as_f64().unwrap();
        assert!((3.0..3.5).contains(&seconds), "{seconds}");
        assert_eq!(sent[0]["streams"], serde_json::json!({}));
        assert_eq!(losses.snapshot().len(), 1);
        let (link, streams) = losses.counts(root).unwrap();
        assert_eq!((link.times, link.seconds), (1, seconds));
        assert!(streams.is_empty());
        assert!(losses.counts("/4".parse().unwrap()).is_none());
    }

    /// A link that drops is a line of the log as it drops, and its return
    /// another, which says how long it was down. A link said down again
    /// while it is down, or up while it is up, is no line.
    #[test]
    fn a_link_that_drops_and_returns_is_two_lines_of_the_log() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let losses = losses(feed);
        let root = DeviceRoute::root();
        losses.link_down(root, Instant::now(), None, None);
        losses.link_up(root, None);
        assert!(lines(&mut rx).is_empty(), "a link never up did not drop");
        let since = Instant::now() - Duration::from_secs(2);
        losses.link_down(root, since, None, None);
        losses.link_down(root, Instant::now(), None, None);
        losses.link_up(root, None);
        losses.link_up(root, None);
        let said = lines(&mut rx);
        assert_eq!(said.len(), 2, "{said:?}");
        assert_eq!(said[0]["level"], "error");
        assert_eq!(said[0]["source"], "proxy");
        assert_eq!(said[0]["route"], "/");
        assert_eq!(
            said[0]["loss"],
            serde_json::json!({"cause": "link", "seconds": null})
        );
        assert_eq!(said[1]["level"], "info");
        assert_eq!(said[1]["loss"]["cause"], "link");
        assert!(said[1]["loss"].get("url").is_none(), "the proxy said none");
        let seconds = said[1]["loss"]["seconds"].as_f64().unwrap();
        assert!((2.0..2.5).contains(&seconds), "{seconds}");

        // With why the device was lost and what the proxy says of the link,
        // the drop's line carries why and where, and the return's where.
        let link = LinkState {
            url: "udp://127.0.0.1:1".to_string(),
        };
        losses.link_down(root, Instant::now(), Some(LostCause::Quiet), Some(&link));
        losses.link_up(root, Some(&link));
        let said = lines(&mut rx);
        assert_eq!(said.len(), 2, "{said:?}");
        assert_eq!(
            said[0]["loss"],
            serde_json::json!({"cause": "link", "seconds": null, "why": "quiet", "url": "udp://127.0.0.1:1"})
        );
        assert_eq!(said[1]["loss"]["url"], "udp://127.0.0.1:1");
        assert!(said[1]["loss"].get("why").is_none());
    }

    /// While a link is down its outage grows with each send.
    #[test]
    fn an_outage_that_lasts_grows_until_the_link_is_up() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let losses = losses(feed);
        losses.link_up(DeviceRoute::root(), None);
        losses.link_down(
            DeviceRoute::root(),
            Instant::now() - Duration::from_secs(10),
            None,
            None,
        );
        losses.send_changed(&Buffer::new(0));
        std::thread::sleep(Duration::from_millis(250));
        losses.send_changed(&Buffer::new(0));
        let seconds: Vec<f64> = heard(&mut rx)
            .iter()
            .map(|sent| sent["link"]["seconds"].as_f64().unwrap())
            .collect();
        assert_eq!(seconds.len(), 2);
        assert!(seconds[0] >= 10.0 && seconds[1] > seconds[0], "{seconds:?}");
    }
}

//! What the hub's connection lost of each device, and where, counted since
//! the hub started. Every page is sent the counts as they grow.
//!
//! A sample can be lost in four places before the hub's history holds it,
//! and [`Lost`] counts the rows of a stream lost in each. Each batch says
//! what was lost before it, in its [`Loss`](twinleaf::data::Loss), which is
//! where the counts come from.
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
//! is one line, with why the proxy lost the device and the link's address
//! as the proxy says them, and its return another, which says how long it
//! was down.
//! The rows a stream lost since the counts were last sent are a line for
//! each place they were lost, but for those lost with the link down, which
//! the link's own lines stand for.

use crate::feed::Feed;
use crate::logbook::{self, Logbook};
use crate::protocol::{Frame, Level, Link, LossLine, Lost, Message, Why};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use twinleaf::device::{LinkState, LostCause};
use twinleaf::{DeviceRoute, SampleBatch};

/// The counts of every device, and the feed they go out on.
pub(crate) struct Losses {
    devices: Mutex<BTreeMap<DeviceRoute, Device>>,
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
    /// The rows lost of each stream, by the stream's name.
    streams: BTreeMap<String, Lost>,
    /// What the last message said, or nothing lost before the first.
    sent: (Link, BTreeMap<String, Lost>),
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
            feed,
            log,
        })
    }

    /// Counts the rows `batch` says were lost before it, by where.
    pub(crate) fn count(&self, batch: &SampleBatch) {
        let Some(loss) = batch.boundary().and_then(|b| b.loss.as_ref()) else {
            return;
        };
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(batch.route()).or_default();
        let stream = batch.schema().stream().name();
        if !device.streams.contains_key(stream) {
            device.streams.insert(stream.to_string(), Lost::default());
        }
        let lost = device.streams.get_mut(stream).expect("the stream");
        lost.skipped += loss.skipped_samples();
        lost.link += loss.link_samples;
        lost.proxy += loss.proxy_samples;
        lost.hub += loss.dropped_samples;
    }

    /// Counts a drop of the link to the device at `route`, which went down
    /// at `since`. Only a link that was up drops, so a device lost before
    /// the hub heard it counts none, and a link that is down drops no
    /// further. `link` is what the proxy says of the link that serves the
    /// device, if it has, which the line of the drop carries.
    pub(crate) fn link_down(&self, route: DeviceRoute, since: Instant, link: Option<&LinkState>) {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(route).or_default();
        if std::mem::take(&mut device.up) {
            device.down = Some(since);
            device.drops += 1;
            let why = link.and_then(|link| link.lost).and_then(why_of);
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
    /// meanwhile in each place, and logs how many rows the hub dropped.
    pub(crate) fn send_changed(&self) {
        let now = Instant::now();
        let mut dropped = 0;
        for (route, device) in self.devices.lock().unwrap().iter_mut() {
            let counts = (device.link(now), device.streams.clone());
            if device.sent == counts {
                continue;
            }
            let hub = |streams: &BTreeMap<String, Lost>| -> u64 {
                streams.values().map(|lost| lost.hub).sum()
            };
            dropped += hub(&counts.1) - hub(&device.sent.1);
            for (stream, lost) in &counts.1 {
                let before = device.sent.1.get(stream).copied().unwrap_or_default();
                self.lines(*route, stream, lost, &before);
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

    /// How often the link to the device at `route` dropped.
    #[cfg(test)]
    pub(crate) fn drops(&self, route: DeviceRoute) -> u64 {
        let devices = self.devices.lock().unwrap();
        devices.get(&route).map_or(0, |device| device.drops)
    }

    /// The rows counted lost of stream `stream` of the device at `route`.
    #[cfg(test)]
    pub(crate) fn of(&self, route: DeviceRoute, stream: &str) -> Lost {
        let devices = self.devices.lock().unwrap();
        let device = devices.get(&route);
        device
            .and_then(|device| device.streams.get(stream).copied())
            .unwrap_or_default()
    }
}

/// Why the proxy lost a device, as the page is told, or `None` for a cause
/// the page has no word for.
fn why_of(cause: LostCause) -> Option<Why> {
    match cause {
        LostCause::Disconnected => Some(Why::Disconnected),
        LostCause::Stopped => Some(Why::Stopped),
        LostCause::FailedToConnect => Some(Why::FailedToConnect),
        LostCause::FailedToReconnect => Some(Why::FailedToReconnect),
        LostCause::Quiet => Some(Why::Quiet),
        _ => None,
    }
}

/// The message of the `counts` of the device at `route`.
fn message(route: DeviceRoute, (link, streams): &(Link, BTreeMap<String, Lost>)) -> Message {
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
        losses.send_changed();
        assert!(heard(&mut rx).is_empty(), "nothing was lost");

        let since = Instant::now() - Duration::from_secs(3);
        losses.link_down(root, since, None);
        losses.send_changed();
        assert!(heard(&mut rx).is_empty(), "a link never up did not drop");
        losses.link_up(root, None);
        losses.link_down(root, since, None);
        losses.link_down(root, Instant::now(), None);
        losses.link_up(root, None);
        losses.link_up(root, None);
        losses.send_changed();
        losses.send_changed();
        let sent = heard(&mut rx);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0]["type"], "loss");
        assert_eq!(sent[0]["route"], "/");
        assert_eq!(sent[0]["link"]["times"], 1);
        let seconds = sent[0]["link"]["seconds"].as_f64().unwrap();
        assert!((3.0..3.5).contains(&seconds), "{seconds}");
        assert_eq!(sent[0]["streams"], serde_json::json!({}));
        assert_eq!(losses.snapshot().len(), 1);
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
        losses.link_down(root, Instant::now(), None);
        losses.link_up(root, None);
        assert!(lines(&mut rx).is_empty(), "a link never up did not drop");
        let since = Instant::now() - Duration::from_secs(2);
        losses.link_down(root, since, None);
        losses.link_down(root, Instant::now(), None);
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

        // With what the proxy says of the link, the drop's line carries why
        // and where, and the return's where.
        let link = |lost| LinkState {
            url: "udp://127.0.0.1:1".to_string(),
            lost,
        };
        for (cause, why) in [
            (LostCause::Disconnected, "disconnected"),
            (LostCause::Stopped, "stopped"),
            (LostCause::FailedToConnect, "failed_to_connect"),
            (LostCause::FailedToReconnect, "failed_to_reconnect"),
            (LostCause::Quiet, "quiet"),
        ] {
            losses.link_down(root, Instant::now(), Some(&link(Some(cause))));
            losses.link_up(root, Some(&link(Some(cause))));
            let said = lines(&mut rx);
            assert_eq!(said.len(), 2, "{said:?}");
            assert_eq!(
                said[0]["loss"],
                serde_json::json!({"cause": "link", "seconds": null, "why": why, "url": "udp://127.0.0.1:1"})
            );
            assert_eq!(said[1]["loss"]["url"], "udp://127.0.0.1:1");
            assert!(said[1]["loss"].get("why").is_none());
        }
        let seconds = said[1]["loss"]["seconds"].as_f64().unwrap();
        assert!((2.0..2.5).contains(&seconds), "{seconds}");
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
        );
        losses.send_changed();
        std::thread::sleep(Duration::from_millis(250));
        losses.send_changed();
        let seconds: Vec<f64> = heard(&mut rx)
            .iter()
            .map(|sent| sent["link"]["seconds"].as_f64().unwrap())
            .collect();
        assert_eq!(seconds.len(), 2);
        assert!(seconds[0] >= 10.0 && seconds[1] > seconds[0], "{seconds:?}");
    }
}

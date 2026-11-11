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

use crate::feed::Feed;
use crate::protocol::{Frame, Link, Lost, Message};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use twinleaf::{DeviceRoute, SampleBatch};

/// The counts of every device, and the feed they go out on.
pub(crate) struct Losses {
    devices: Mutex<BTreeMap<DeviceRoute, Device>>,
    feed: Arc<Feed>,
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
    pub(crate) fn new(feed: Arc<Feed>) -> Arc<Losses> {
        Arc::new(Losses {
            devices: Mutex::new(BTreeMap::new()),
            feed,
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
    /// further.
    pub(crate) fn link_down(&self, route: DeviceRoute, since: Instant) {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(route).or_default();
        if std::mem::take(&mut device.up) {
            device.down = Some(since);
            device.drops += 1;
        }
    }

    /// Notes that the device at `route` is heard, which ends the outage of
    /// its link if it is down.
    pub(crate) fn link_up(&self, route: DeviceRoute) {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.entry(route).or_default();
        device.up = true;
        if let Some(since) = device.down.take() {
            device.seconds += since.elapsed().as_secs_f64();
        }
    }

    /// Sends the counts of each device whose counts changed since they were
    /// last sent, and logs how many rows the hub dropped meanwhile.
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
            self.feed.message(&message(*route, &counts));
            device.sent = counts;
        }
        if dropped > 0 {
            log::warn!("The web page's hub fell behind and dropped {dropped} samples");
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
    use std::time::Duration;

    /// The loss messages `rx` heard, as JSON.
    fn heard(rx: &mut tokio::sync::broadcast::Receiver<Arc<Frame>>) -> Vec<serde_json::Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str(text).unwrap(),
                Frame::Binary(_) => panic!("a loss is a message"),
            })
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
        let losses = Losses::new(feed);
        let root = DeviceRoute::root();
        losses.send_changed();
        assert!(heard(&mut rx).is_empty(), "nothing was lost");

        let since = Instant::now() - Duration::from_secs(3);
        losses.link_down(root, since);
        losses.send_changed();
        assert!(heard(&mut rx).is_empty(), "a link never up did not drop");
        losses.link_up(root);
        losses.link_down(root, since);
        losses.link_down(root, Instant::now());
        losses.link_up(root);
        losses.link_up(root);
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

    /// While a link is down its outage grows with each send.
    #[test]
    fn an_outage_that_lasts_grows_until_the_link_is_up() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let losses = Losses::new(feed);
        losses.link_up(DeviceRoute::root());
        losses.link_down(
            DeviceRoute::root(),
            Instant::now() - Duration::from_secs(10),
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

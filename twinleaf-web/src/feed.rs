//! The channel of changes every page hears. It numbers each change one more
//! than the last, so a page can tell it missed one, and a page that
//! subscribes learns the number it starts after.

use crate::protocol::{Frame, Message};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Frames a page may fall behind before it is sent a snapshot instead.
const BACKLOG: usize = 1024;

pub(crate) struct Feed {
    /// The number of the last change sent, and the channel. They are held
    /// under one lock, so the numbers go out in order and a subscriber's
    /// starting number is exact.
    numbered: Mutex<(u64, broadcast::Sender<Arc<Frame>>)>,
}

impl Feed {
    pub(crate) fn new() -> Arc<Feed> {
        Arc::new(Feed {
            numbered: Mutex::new((0, broadcast::channel(BACKLOG).0)),
        })
    }

    /// Sends `message` as the next change.
    pub(crate) fn message(&self, message: &Message) {
        self.send(|seq| message.numbered(seq));
    }

    /// Sends the binary frame `encode` makes of the next change's number.
    pub(crate) fn binary(&self, encode: impl FnOnce(u64) -> Vec<u8>) {
        self.send(|seq| Frame::Binary(encode(seq)));
    }

    fn send(&self, frame: impl FnOnce(u64) -> Frame) {
        let mut numbered = self.numbered.lock().unwrap();
        numbered.0 += 1;
        let frame = frame(numbered.0);
        let _ = numbered.1.send(Arc::new(frame));
    }

    /// The number of the last change sent, and a receiver of every change
    /// after it.
    pub(crate) fn subscribe(&self) -> (u64, broadcast::Receiver<Arc<Frame>>) {
        let numbered = self.numbered.lock().unwrap();
        (numbered.0, numbered.1.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subscriber_hears_the_changes_after_the_number_it_starts_at() {
        let feed = Feed::new();
        feed.message(&Message::Clear);
        let (last, mut rx) = feed.subscribe();
        assert_eq!(last, 1);
        feed.binary(|seq| vec![seq as u8]);
        feed.message(&Message::Config { pps: 5.0 });
        assert_eq!(*rx.try_recv().unwrap(), Frame::Binary(vec![2]));
        let Frame::Text(text) = &*rx.try_recv().unwrap() else {
            panic!("a message is text")
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text).unwrap(),
            serde_json::json!({"type": "config", "seq": 3, "pps": 5.0})
        );
    }
}

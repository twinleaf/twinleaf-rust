//! tokio's `StreamMap`, as the turmoil tests have it. tokio's starts each
//! poll at a stream drawn from a generator that a process-wide counter seeds,
//! so a run made twice would draw two ways. This one starts each poll one
//! stream further on than the last.

use futures_util::Stream;
use std::borrow::Borrow;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A set of streams, each under a key, that yields each item with the key
/// of its stream. Each poll starts one stream further on than the last, so
/// no stream starves the others, and goes round the rest from there. A
/// stream that ends is removed. The set ends once it holds no stream.
#[derive(Debug)]
pub(crate) struct StreamMap<K, V> {
    entries: Vec<(K, V)>,
    /// How many polls the set has had.
    polls: usize,
}

impl<K, V> Default for StreamMap<K, V> {
    fn default() -> Self {
        StreamMap {
            entries: Vec::new(),
            polls: 0,
        }
    }
}

impl<K: PartialEq, V> StreamMap<K, V> {
    /// Returns a set that holds no stream.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the set holds no stream.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Removes every stream.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    /// Adds `stream` under `key`. Returns the stream it replaces under
    /// that key, if there was one.
    pub(crate) fn insert(&mut self, key: K, stream: V) -> Option<V> {
        let replaced = self.remove(&key);
        self.entries.push((key, stream));
        replaced
    }

    /// Removes the stream under `key`, and returns it if there was one.
    /// The last stream takes its place.
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        let at = self
            .entries
            .iter()
            .position(|(held, _)| held.borrow() == key)?;
        Some(self.entries.swap_remove(at).1)
    }
}

impl<K: Clone + Unpin, V: Stream + Unpin> Stream for StreamMap<K, V> {
    type Item = (K, V::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polls = self.polls;
        self.polls = polls.wrapping_add(1);
        let entries = &mut self.entries;
        let start = polls.checked_rem(entries.len()).unwrap_or(0);
        let mut at = start;
        for _ in 0..entries.len() {
            let (key, stream) = &mut entries[at];
            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some((key.clone(), item))),
                Poll::Ready(None) => {
                    entries.swap_remove(at);
                    if at == entries.len() {
                        at = 0;
                    } else if at < start && start <= entries.len() {
                        // The stream swapped into `at` was polled already.
                        at = (at + 1) % entries.len();
                    }
                }
                Poll::Pending => at = (at + 1) % entries.len(),
            }
        }
        match entries.is_empty() {
            true => Poll::Ready(None),
            false => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{self, StreamExt};
    use std::task::Waker;

    /// A stream that ends at once and one that yields are polled in one
    /// turn, and the set yields the item and drops the ended stream.
    #[test]
    fn an_ended_stream_is_dropped_and_the_others_still_yield() {
        let mut map = StreamMap::new();
        map.insert(1, stream::iter(Vec::<u8>::new()).boxed());
        map.insert(2, stream::iter(vec![7u8]).boxed());
        let mut cx = Context::from_waker(Waker::noop());
        let mut taken = Vec::new();
        while let Poll::Ready(Some(item)) = Pin::new(&mut map).poll_next(&mut cx) {
            taken.push(item);
        }
        assert_eq!(taken, [(2, 7)]);
        assert!(map.is_empty());
    }

    /// Inserting under a key held already replaces its stream.
    #[test]
    fn an_insert_under_a_held_key_replaces_its_stream() {
        let mut map = StreamMap::new();
        assert!(map.insert(1, stream::iter(vec![1u8])).is_none());
        assert!(map.insert(1, stream::iter(vec![2u8])).is_some());
        let mut cx = Context::from_waker(Waker::noop());
        let first = Pin::new(&mut map).poll_next(&mut cx);
        assert_eq!(first, Poll::Ready(Some((1, 2))));
    }
}

//! The crate's randomness. Each thread draws from a generator of its own,
//! a [`StreamMap`] starts each poll at a stream picked at random, and a
//! [`HashMap`] or [`HashSet`] walks its entries in an order its hash keys
//! decide.
//!
//! A normal build seeds a thread's generator from the random keys the
//! standard library makes for a hash map. Its `StreamMap` is tokio's, and
//! its `HashMap` and `HashSet` are the standard library's with random keys.
//! The crate's tests built with `--cfg turmoil` seed the generator with the
//! seed of the scenario the thread runs, which `seed` sets. Their
//! `StreamMap` is a copy of tokio's that picks its first stream with the
//! generator, and their `HashMap` and `HashSet` hash with fixed keys. A
//! scenario run twice with one seed then draws the same numbers in the same
//! order and walks each map in the same order. The command is in
//! `crate::net`.
//!
//! The crate's clippy configuration refuses `RandomState::new`, tokio's
//! `StreamMap` and the standard library's `HashMap` and `HashSet` anywhere
//! else, so every hash map and set the crate makes is one of this module's.
//! The public `ExportStats` is the one exception, as its sets of stream
//! names are part of the crate's API. A map whose iteration order reaches a
//! send, an event or a wakeup is still an ordered one, since a normal build
//! walks a hash map in a random order.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cell::Cell;

#[cfg(not(all(test, turmoil)))]
pub(crate) type StreamMap<K, V> = tokio_stream::StreamMap<K, V>;
#[cfg(all(test, turmoil))]
pub(crate) use seeded::StreamMap;

/// A hash map that hashes with the build's [`Keys`]. Make one with
/// `default` or `collect`, which every build has.
pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, Keys>;

/// A hash set that hashes with the build's [`Keys`]. Make one with
/// `default` or `collect`, which every build has.
pub(crate) type HashSet<T> = std::collections::HashSet<T, Keys>;

/// The hash keys of a [`HashMap`] or [`HashSet`], random for each map.
#[cfg(not(all(test, turmoil)))]
type Keys = std::collections::hash_map::RandomState;

/// The hash keys of a [`HashMap`] or [`HashSet`], the same for every map in
/// every run of one build.
#[cfg(all(test, turmoil))]
type Keys = std::hash::BuildHasherDefault<std::hash::DefaultHasher>;

thread_local! {
    /// The state of this thread's generator.
    static STATE: Cell<u64> = Cell::new(first_state());
}

/// Returns the state a thread's generator starts from.
#[cfg(not(all(test, turmoil)))]
fn first_state() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(())
}

/// Returns the state a thread's generator starts from until a scenario seeds
/// it.
#[cfg(all(test, turmoil))]
fn first_state() -> u64 {
    0
}

/// Seeds this thread's generator with `seed`.
#[cfg(all(test, turmoil))]
pub(crate) fn seed(seed: u64) {
    STATE.with(|state| state.set(seed));
}

/// Returns the next number of this thread's generator, which is a
/// `SplitMix64`.
pub(crate) fn next_u64() -> u64 {
    STATE.with(|state| {
        let next = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        state.set(next);
        let mut mixed = next;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    })
}

/// The `StreamMap` of the turmoil tests.
#[cfg(all(test, turmoil))]
mod seeded {
    use super::next_u64;
    use futures_util::Stream;
    use std::borrow::Borrow;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Returns a number below `n` from this thread's generator, or 0 if `n`
    /// is 0.
    pub(super) fn below(n: usize) -> usize {
        ((u128::from(next_u64()) * n as u128) >> 64) as usize
    }

    /// A set of streams, each under a key, that yields each item with the
    /// key of its stream. Each poll starts at a stream [`below`] draws, so no
    /// stream starves the others, and goes round the rest from there. A
    /// stream that ends is removed. The set ends once it holds no stream.
    #[derive(Debug)]
    pub(crate) struct StreamMap<K, V> {
        entries: Vec<(K, V)>,
    }

    impl<K, V> Default for StreamMap<K, V> {
        fn default() -> Self {
            StreamMap {
                entries: Vec::new(),
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
            let entries = &mut self.entries;
            let start = below(entries.len());
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

    // The tests sit in a module named for turmoil, so the turmoil tier's
    // filter runs them.
    mod turmoil {
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

        /// A number drawn below `n` is below it.
        #[test]
        fn a_number_drawn_below_n_is_below_it() {
            for n in 1..50 {
                assert!(below(n) < n);
            }
            assert_eq!(below(0), 0);
        }
    }
}

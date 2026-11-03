//! A channel with the interface of flume's, built on shuttle's `Mutex` and
//! `Condvar`, so that shuttle schedules each of its operations.
//!
//! Every operation takes the channel's lock, which is a scheduling point. A
//! blocking send or receive waits on the channel's condvar, and an async
//! receive leaves its waker, which the next change to the channel wakes. The
//! parent module says where it differs from flume.

use ::shuttle::sync::{Condvar, Mutex, MutexGuard};
use futures_util::stream::{FusedStream, Stream};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// The item a send could not deliver, as every receiver is gone.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

/// Why a send that does not block delivered nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel had no room.
    Full(T),
    /// Every receiver is gone.
    Disconnected(T),
}

/// Why a receive returned nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// Every sender is gone and the channel is empty.
    Disconnected,
}

/// Why a receive that does not block returned nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// Every sender is gone and the channel is empty.
    Disconnected,
}

/// Why a receive with a time bound returned nothing. The stand-in never
/// returns [`Timeout`](Self::Timeout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
    /// The time bound passed.
    Timeout,
    /// Every sender is gone and the channel is empty.
    Disconnected,
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Disconnected(_) => f.write_str("Disconnected(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed channel")
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("sending on a full channel"),
            TrySendError::Disconnected(_) => f.write_str("sending on a closed channel"),
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a closed channel")
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("receiving on an empty channel"),
            TryRecvError::Disconnected => f.write_str("receiving on a closed channel"),
        }
    }
}

impl fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvTimeoutError::Timeout => f.write_str("timed out waiting on a channel"),
            RecvTimeoutError::Disconnected => f.write_str("receiving on a closed channel"),
        }
    }
}

impl<T> std::error::Error for SendError<T> {}
impl<T> std::error::Error for TrySendError<T> {}
impl std::error::Error for RecvError {}
impl std::error::Error for TryRecvError {}
impl std::error::Error for RecvTimeoutError {}

/// What one channel holds, and who holds it.
struct State<T> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    senders: usize,
    receivers: usize,
    /// The wakers of the async receives waiting for a change.
    wakers: Vec<Waker>,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Releases `state`, then wakes every waiting send and receive.
    fn changed(&self, mut state: MutexGuard<'_, State<T>>) {
        let wakers = std::mem::take(&mut state.wakers);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        self.changed.notify_all();
    }

    /// Waits on the condvar for the next change, holding `state` again
    /// once it comes.
    fn wait<'a>(&self, state: MutexGuard<'a, State<T>>) -> MutexGuard<'a, State<T>> {
        self.changed
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Takes the next item, or says why there is none. With `cx`, a
    /// receive that finds none leaves its waker.
    fn poll_take(&self, cx: Option<&mut Context<'_>>) -> Poll<Result<T, RecvError>> {
        let mut state = self.lock();
        if let Some(item) = state.queue.pop_front() {
            self.changed(state);
            return Poll::Ready(Ok(item));
        }
        if state.senders == 0 {
            return Poll::Ready(Err(RecvError::Disconnected));
        }
        if let Some(cx) = cx {
            if !state.wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                state.wakers.push(cx.waker().clone());
            }
        }
        Poll::Pending
    }

    /// Whether `state` has no room for another item.
    fn full(state: &State<T>) -> bool {
        state
            .capacity
            .is_some_and(|capacity| state.queue.len() >= capacity)
    }
}

/// The sending end of a channel.
pub struct Sender<T>(Arc<Shared<T>>);

/// The receiving end of a channel.
pub struct Receiver<T>(Arc<Shared<T>>);

fn channel<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            capacity,
            senders: 1,
            receivers: 1,
            wakers: Vec::new(),
        }),
        changed: Condvar::new(),
    });
    (Sender(Arc::clone(&shared)), Receiver(shared))
}

/// Returns a channel with room for `capacity` items.
///
/// # Panics
///
/// Panics if `capacity` is zero.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(
        capacity > 0,
        "the shuttle stand-in has no rendezvous channel"
    );
    channel(Some(capacity))
}

/// Returns a channel with room for any number of items.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(None)
}

impl<T> Sender<T> {
    /// Sends `item` if the channel has room.
    ///
    /// # Errors
    ///
    /// Returns the item if the channel is full or every receiver is gone.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let mut state = self.0.lock();
        if state.receivers == 0 {
            return Err(TrySendError::Disconnected(item));
        }
        if Shared::full(&state) {
            return Err(TrySendError::Full(item));
        }
        state.queue.push_back(item);
        self.0.changed(state);
        Ok(())
    }

    /// Sends `item`, waiting for room.
    ///
    /// # Errors
    ///
    /// Returns the item once every receiver is gone.
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let mut state = self.0.lock();
        loop {
            if state.receivers == 0 {
                return Err(SendError(item));
            }
            if !Shared::full(&state) {
                state.queue.push_back(item);
                self.0.changed(state);
                return Ok(());
            }
            state = self.0.wait(state);
        }
    }

    /// Sends `item` as [`send`](Self::send) does, blocking the task that
    /// awaits it while the channel is full.
    ///
    /// # Errors
    ///
    /// Returns the item once every receiver is gone.
    pub async fn send_async(&self, item: T) -> Result<(), SendError<T>> {
        self.send(item)
    }

    /// Returns how many items the channel holds.
    pub fn len(&self) -> usize {
        self.0.lock().queue.len()
    }

    /// Returns `true` if the channel holds no item.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns how many receivers the channel has.
    pub fn receiver_count(&self) -> usize {
        self.0.lock().receivers
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.lock().senders += 1;
        Sender(Arc::clone(&self.0))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.senders -= 1;
        self.0.changed(state);
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Receiver<T> {
    /// Takes the next item if one is queued.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] if none is, and
    /// [`TryRecvError::Disconnected`] once the channel is empty and every
    /// sender is gone.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.0.poll_take(None) {
            Poll::Ready(Ok(item)) => Ok(item),
            Poll::Ready(Err(RecvError::Disconnected)) => Err(TryRecvError::Disconnected),
            Poll::Pending => Err(TryRecvError::Empty),
        }
    }

    /// Waits for the next item.
    ///
    /// # Errors
    ///
    /// Fails once the channel is empty and every sender is gone.
    pub fn recv(&self) -> Result<T, RecvError> {
        let mut state = self.0.lock();
        loop {
            if let Some(item) = state.queue.pop_front() {
                self.0.changed(state);
                return Ok(item);
            }
            if state.senders == 0 {
                return Err(RecvError::Disconnected);
            }
            state = self.0.wait(state);
        }
    }

    /// Waits for the next item, and never times out.
    ///
    /// # Errors
    ///
    /// Fails as [`recv`](Self::recv) does.
    pub fn recv_timeout(&self, _timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.recv().map_err(|_| RecvTimeoutError::Disconnected)
    }

    /// Waits for the next item, and never times out.
    ///
    /// # Errors
    ///
    /// Fails as [`recv`](Self::recv) does.
    pub fn recv_deadline(&self, _deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.recv().map_err(|_| RecvTimeoutError::Disconnected)
    }

    /// Returns a future of the next item, which borrows the receiver.
    pub fn recv_async(&self) -> RecvFut<'_, T> {
        RecvFut(Held::Borrowed(self))
    }

    /// Returns a future of the next item, which owns the receiver.
    pub fn into_recv_async(self) -> RecvFut<'static, T> {
        RecvFut(Held::Owned(self))
    }

    /// Returns a stream of the items, which borrows the receiver.
    pub fn stream(&self) -> RecvStream<'_, T> {
        RecvStream(Held::Borrowed(self), false)
    }

    /// Returns a stream of the items, which owns the receiver.
    pub fn into_stream(self) -> RecvStream<'static, T> {
        RecvStream(Held::Owned(self), false)
    }

    /// Returns an iterator over the items queued now.
    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter(self)
    }

    /// Takes every item queued now, and returns them in order.
    pub fn drain(&self) -> std::collections::vec_deque::IntoIter<T> {
        let mut state = self.0.lock();
        let taken = std::mem::take(&mut state.queue);
        self.0.changed(state);
        taken.into_iter()
    }

    /// Returns how many items the channel holds.
    pub fn len(&self) -> usize {
        self.0.lock().queue.len()
    }

    /// Returns `true` if the channel holds no item.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` once every sender is gone.
    pub fn is_disconnected(&self) -> bool {
        self.0.lock().senders == 0
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.0.lock().receivers += 1;
        Receiver(Arc::clone(&self.0))
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.receivers -= 1;
        self.0.changed(state);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// A receiver a future or stream borrows or owns.
enum Held<'a, T> {
    Borrowed(&'a Receiver<T>),
    Owned(Receiver<T>),
}

impl<T> Held<'_, T> {
    fn receiver(&self) -> &Receiver<T> {
        match self {
            Held::Borrowed(receiver) => receiver,
            Held::Owned(receiver) => receiver,
        }
    }
}

/// The items queued at each call to `next`, until the channel is empty.
pub struct TryIter<'a, T>(&'a Receiver<T>);

impl<T> fmt::Debug for TryIter<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TryIter").finish_non_exhaustive()
    }
}

impl<T> Iterator for TryIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.0.try_recv().ok()
    }
}

/// A future of a channel's next item.
pub struct RecvFut<'a, T>(Held<'a, T>);

impl<T> Future for RecvFut<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.receiver().0.poll_take(Some(cx))
    }
}

impl<T> fmt::Debug for RecvFut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvFut").finish_non_exhaustive()
    }
}

/// A stream of a channel's items, which ends once the channel is empty and
/// every sender is gone.
pub struct RecvStream<'a, T>(Held<'a, T>, bool);

impl<T> Stream for RecvStream<'_, T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let polled = self.0.receiver().0.poll_take(Some(cx)).map(Result::ok);
        if let Poll::Ready(None) = polled {
            self.1 = true;
        }
        polled
    }
}

impl<T> FusedStream for RecvStream<'_, T> {
    fn is_terminated(&self) -> bool {
        self.1
    }
}

impl<T> fmt::Debug for RecvStream<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvStream").finish_non_exhaustive()
    }
}

/// A wait on several receivers at once, which builds and panics when it
/// waits.
pub struct Selector<'a, R>(PhantomData<&'a R>);

/// The time bound of a [`Selector`] passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectError;

impl<'a, R> Selector<'a, R> {
    /// Returns a selector over no receiver.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Selector(PhantomData)
    }

    /// Adds `receiver`, whose item `mapper` would map.
    pub fn recv<T, F>(self, _receiver: &'a Receiver<T>, _mapper: F) -> Self
    where
        F: FnMut(Result<T, RecvError>) -> R + 'a,
    {
        self
    }

    /// Panics, as the stand-in has no selector.
    pub fn wait(self) -> R {
        unimplemented!("the shuttle stand-in has no selector")
    }

    /// Panics, as the stand-in has no selector.
    pub fn wait_deadline(self, _deadline: Instant) -> Result<R, SelectError> {
        unimplemented!("the shuttle stand-in has no selector")
    }
}

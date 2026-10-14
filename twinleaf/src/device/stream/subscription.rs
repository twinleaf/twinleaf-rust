//! Owned subscriptions. A view hands out a [`Receiver`], the pump fills its
//! [`Sink`], and a [`Scope`] filters what passes between them.

use super::Stream;
use crate::device::session::Handle;
use crate::proto::DeviceRoute;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Why a receive returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecvError {
    /// The subscriber fell behind, and the pump dropped this many items.
    /// Recoverable. Receiving again returns the items after the gap.
    #[error("skipped {0} items")]
    Lagged(u64),
    /// The stream ended, for example because the connection to the proxy
    /// closed. Terminal.
    #[error("the stream ended")]
    Disconnected,
    /// The proxy stopped, which ended the stream. Terminal.
    #[error("the proxy stopped")]
    Stopped,
    /// The time bound passed. Only the time-bounded receives report this.
    #[error("timed out waiting for an item")]
    Timeout,
}

/// An entry on a subscription's queue.
#[derive(Debug)]
pub enum Queued<T> {
    /// An item.
    Item(T),
    /// The count of items the pump dropped while the subscriber was behind.
    Gap(u64),
}

/// An owned subscription to one subtree's stream.
///
/// Holding it keeps the connection and its stream running after every view
/// is dropped.
pub struct Receiver<T> {
    items: flume::Receiver<Queued<T>>,
    /// Dropped with the receiver, which wakes the pump to drop its sink.
    _lifeline: oneshot::Sender<Infallible>,
    /// The stream this came from, which it keeps running and asks why it
    /// ended. `None` only until [`tied_to`](Self::tied_to) sets it.
    stream: Option<Arc<Stream>>,
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("queued", &self.items.len())
            .finish_non_exhaustive()
    }
}

impl<T> Receiver<T> {
    /// Blocks until the next item arrives.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] after a gap, and [`RecvError::Stopped`]
    /// or [`RecvError::Disconnected`] once the stream has ended. Does not return
    /// [`RecvError::Timeout`].
    pub fn recv(&self) -> Result<T, RecvError> {
        self.resolve(self.items.recv())
    }

    /// Waits until the next item arrives, without blocking the thread.
    ///
    /// # Errors
    ///
    /// Fails as [`recv`](Self::recv) does.
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        self.resolve(self.items.recv_async().await)
    }

    /// Returns the next item if one is queued, or `Ok(None)` if none is.
    /// Does not block.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] after a gap, and [`RecvError::Stopped`]
    /// or [`RecvError::Disconnected`] once the stream has ended.
    pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
        match self.items.try_recv() {
            Ok(Queued::Item(item)) => Ok(Some(item)),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Err(self.ended()),
        }
    }

    /// Blocks until the next item arrives or `timeout` passes.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Timeout`] if nothing arrives in time. Also fails
    /// as [`recv`](Self::recv) does.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        self.resolve_timed(self.items.recv_timeout(timeout))
    }

    /// Blocks until the next item arrives or `deadline` passes.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Timeout`] if nothing arrives in time, and at once
    /// if `deadline` has already passed, even when items are queued. Also
    /// fails as [`recv`](Self::recv) does.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvError> {
        if Instant::now() >= deadline {
            return Err(RecvError::Timeout);
        }
        self.resolve_timed(self.items.recv_deadline(deadline))
    }

    /// Converts what a receive on [`as_flume`](Self::as_flume) yields into
    /// an item or a [`RecvError`].
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] for a gap, and [`RecvError::Stopped`] or
    /// [`RecvError::Disconnected`] for a disconnected channel.
    pub fn resolve(&self, received: Result<Queued<T>, flume::RecvError>) -> Result<T, RecvError> {
        match received {
            Ok(Queued::Item(item)) => Ok(item),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Err(flume::RecvError::Disconnected) => Err(self.ended()),
        }
    }

    fn resolve_timed(
        &self,
        received: Result<Queued<T>, flume::RecvTimeoutError>,
    ) -> Result<T, RecvError> {
        match received {
            Ok(item) => self.resolve(Ok(item)),
            Err(flume::RecvTimeoutError::Timeout) => Err(RecvError::Timeout),
            Err(flume::RecvTimeoutError::Disconnected) => Err(self.ended()),
        }
    }

    /// Returns why the stream ended, once its channel has disconnected.
    fn ended(&self) -> RecvError {
        // The session records that the proxy stopped before the pump drops
        // the sink, and the disconnect orders that record ahead of this read.
        match &self.stream {
            Some(stream) if stream.root.session().stopped() => RecvError::Stopped,
            Some(_) | None => RecvError::Disconnected,
        }
    }

    /// Returns the underlying channel, to wait on alongside other sources
    /// with a [`flume::Selector`]. Pass what it yields to
    /// [`resolve`](Self::resolve). To await it alone, use
    /// [`recv_async`](Self::recv_async).
    pub fn as_flume(&self) -> &flume::Receiver<Queued<T>> {
        &self.items
    }

    /// Ties this subscription to `stream`, which it then keeps running.
    pub(super) fn tied_to(mut self, stream: &Arc<Stream>) -> Receiver<T> {
        self.stream = Some(Arc::clone(stream));
        self
    }
}

/// The subtree one view covers, `depth` levels below `route`.
#[derive(Clone, Copy)]
pub(crate) struct Scope {
    pub(super) route: DeviceRoute,
    pub(super) depth: usize,
}

impl Scope {
    /// Returns the scope of a view's session handle.
    pub(crate) fn of(handle: &Handle) -> Scope {
        Scope {
            route: handle.scope(),
            depth: handle.depth(),
        }
    }

    /// Returns the scope of exactly `route`.
    pub(crate) fn point(route: DeviceRoute) -> Scope {
        Scope { route, depth: 0 }
    }

    /// Returns the scope of `route` and every route below it.
    pub(crate) fn subtree(route: DeviceRoute) -> Scope {
        Scope {
            route,
            depth: usize::MAX,
        }
    }

    /// Returns `true` if `route` lies within this scope.
    pub(super) fn covers(&self, route: DeviceRoute) -> bool {
        self.route
            .relative_route(&route)
            .is_ok_and(|below| below.len() <= self.depth)
    }

    /// Returns `true` if the two scopes share any route, that is if either
    /// root lies inside the other.
    pub(super) fn intersects(&self, other: &Scope) -> bool {
        self.covers(other.route) || other.covers(self.route)
    }
}

/// One subscriber's queue, with its scope and the count of items it has
/// missed.
pub(super) struct Sink<T> {
    items: flume::Sender<Queued<T>>,
    /// Closes when the subscriber drops its receiver, waking the pump.
    pub(super) alive: oneshot::Receiver<Infallible>,
    pub(super) scope: Scope,
    skipped: u64,
}

impl<T> Sink<T> {
    pub(super) fn new(capacity: usize, scope: Scope) -> (Sink<T>, Receiver<T>) {
        let (items, received) = flume::bounded(capacity);
        let (lifeline, alive) = oneshot::channel();
        (
            Sink {
                items,
                alive,
                scope,
                skipped: 0,
            },
            Receiver {
                items: received,
                _lifeline: lifeline,
                stream: None,
            },
        )
    }

    /// Offers an item concerning `scope`, sent only if `scope` overlaps this
    /// subscriber's scope. Returns `false` once the subscriber is gone.
    pub(super) fn offer(&mut self, scope: Scope, item: impl FnOnce() -> T) -> bool {
        if self.scope.intersects(&scope) {
            self.send(item())
        } else {
            true
        }
    }

    /// Sends one item, or counts it as missed if the queue is full. Returns
    /// `false` once the subscriber is gone.
    pub(super) fn send(&mut self, item: T) -> bool {
        if !self.report_lag() {
            return false;
        }
        match self.items.try_send(Queued::Item(item)) {
            Ok(()) => true,
            Err(flume::TrySendError::Full(_)) => {
                self.skipped += 1;
                true
            }
            Err(flume::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Sends the count of missed items once the queue has room. Returns
    /// `false` once the subscriber is gone.
    pub(super) fn report_lag(&mut self) -> bool {
        if self.skipped == 0 {
            return true;
        }
        match self.items.try_send(Queued::Gap(self.skipped)) {
            Ok(()) => {
                self.skipped = 0;
                true
            }
            Err(flume::TrySendError::Full(_)) => true,
            Err(flume::TrySendError::Disconnected(_)) => false,
        }
    }
}

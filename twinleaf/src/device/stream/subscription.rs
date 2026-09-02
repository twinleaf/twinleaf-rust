//! Owned subscriptions: the [`Receiver`] a view hands out, the [`Sink`] the
//! pump fills, and the [`Scope`] that filters between them.

use super::Stream;
use crate::tio::proto::DeviceRoute;
use crate::tio::proxy;
use crossbeam::channel;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why a receive returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecvError {
    /// The subscriber fell behind and the pump shed this many items.
    /// Recoverable: receive again for the items that follow the gap.
    #[error("skipped {0} items")]
    Lagged(u64),
    /// The pump stopped. Terminal.
    #[error("the stream ended")]
    Disconnected,
    /// The time bound passed with nothing queued. Only the time-bounded
    /// receives report this.
    #[error("timed out waiting for an item")]
    Timeout,
}

/// What travels on a subscription's queue: an item, or the count of items the
/// pump shed while the subscriber was behind.
pub enum Queued<T> {
    Item(T),
    Gap(u64),
}

/// An owned subscription to one subtree's stream. Holding it keeps the pump
/// running even after every view is gone; dropping it releases both.
pub struct Receiver<T> {
    items: channel::Receiver<Queued<T>>,
    _lifeline: channel::Sender<()>,
    /// The stream this was minted from, kept alive so the pump outlives the
    /// views. `None` only between minting the sink and tying it to its stream.
    _stream: Option<Arc<Stream>>,
}

impl<T> Receiver<T> {
    /// Block until the next item. Never [`RecvError::Timeout`].
    pub fn recv(&self) -> Result<T, RecvError> {
        self.resolve(self.items.recv())
    }

    /// The next item if one is already queued, `Ok(None)` if none is.
    pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
        match self.items.try_recv() {
            Ok(Queued::Item(item)) => Ok(Some(item)),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Err(channel::TryRecvError::Empty) => Ok(None),
            Err(channel::TryRecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    /// Block for at most `timeout`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        self.resolve_timed(self.items.recv_timeout(timeout))
    }

    /// Block until `deadline`. The bound holds even when the queue never runs
    /// dry, so a saturated subscription cannot deliver forever.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvError> {
        if Instant::now() >= deadline {
            return Err(RecvError::Timeout);
        }
        self.resolve_timed(self.items.recv_deadline(deadline))
    }

    /// Flatten what a `select!` receive arm on [`receiver`](Self::receiver)
    /// yields into the one error family.
    pub fn resolve(&self, received: Result<Queued<T>, channel::RecvError>) -> Result<T, RecvError> {
        match received {
            Ok(Queued::Item(item)) => Ok(item),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Err(channel::RecvError) => Err(RecvError::Disconnected),
        }
    }

    fn resolve_timed(
        &self,
        received: Result<Queued<T>, channel::RecvTimeoutError>,
    ) -> Result<T, RecvError> {
        match received {
            Ok(item) => self.resolve(Ok(item)),
            Err(channel::RecvTimeoutError::Timeout) => Err(RecvError::Timeout),
            Err(channel::RecvTimeoutError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    /// The underlying channel, to `select!` on alongside other sources. Pass
    /// what an arm yields to [`resolve`](Self::resolve).
    pub fn receiver(&self) -> &channel::Receiver<Queued<T>> {
        &self.items
    }

    /// Hold the stream that minted this subscription, so receiving from it
    /// keeps the pump running whatever became of the views.
    pub(super) fn tied_to(mut self, stream: &Arc<Stream>) -> Receiver<T> {
        self._stream = Some(Arc::clone(stream));
        self
    }
}

/// The subtree one view covers: `depth` levels below `route`.
#[derive(Clone, Copy)]
pub(crate) struct Scope {
    pub(super) route: DeviceRoute,
    pub(super) depth: usize,
}

impl Scope {
    /// The subtree an RPC capability already describes.
    pub(crate) fn of(endpoint: &proxy::RpcEndpoint) -> Scope {
        Scope {
            route: endpoint.scope(),
            depth: endpoint.depth(),
        }
    }

    /// The singleton region: exactly this route.
    pub(crate) fn point(route: DeviceRoute) -> Scope {
        Scope { route, depth: 0 }
    }

    /// The route and everything below it, however deep.
    pub(crate) fn subtree(route: DeviceRoute) -> Scope {
        Scope {
            route,
            depth: usize::MAX,
        }
    }

    pub(super) fn covers(&self, route: DeviceRoute) -> bool {
        self.route
            .relative_route(&route)
            .is_ok_and(|below| below.len() <= self.depth)
    }

    /// Whether the two regions share any route: on a tree, whether either root
    /// lies inside the other.
    pub(super) fn intersects(&self, other: &Scope) -> bool {
        self.covers(other.route) || other.covers(self.route)
    }
}

/// One subscriber's queue: what it covers, what it will take, and what it has
/// missed.
pub(super) struct Sink<T> {
    items: channel::Sender<Queued<T>>,
    /// Disconnects when the subscriber drops its receiver, waking the pump.
    pub(super) alive: channel::Receiver<()>,
    pub(super) scope: Scope,
    skipped: u64,
}

impl<T> Sink<T> {
    pub(super) fn new(capacity: usize, scope: Scope) -> (Sink<T>, Receiver<T>) {
        let (items, received) = channel::bounded(capacity);
        let (lifeline, alive) = channel::bounded(0);
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
                _stream: None,
            },
        )
    }

    /// Offer an item concerning `scope`, taken only when it intersects this
    /// subscriber's own. False once the subscriber is gone.
    pub(super) fn offer(&mut self, scope: Scope, item: impl FnOnce() -> T) -> bool {
        if self.scope.intersects(&scope) {
            self.send(item())
        } else {
            true
        }
    }

    /// Take one item, counting it as lag if the subscriber has no room.
    /// False once the subscriber is gone.
    pub(super) fn send(&mut self, item: T) -> bool {
        if !self.report_lag() {
            return false;
        }
        match self.items.try_send(Queued::Item(item)) {
            Ok(()) => true,
            Err(channel::TrySendError::Full(_)) => {
                self.skipped += 1;
                true
            }
            Err(channel::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Tell the subscriber what it missed, once its queue has room again.
    pub(super) fn report_lag(&mut self) -> bool {
        if self.skipped == 0 {
            return true;
        }
        match self.items.try_send(Queued::Gap(self.skipped)) {
            Ok(()) => {
                self.skipped = 0;
                true
            }
            Err(channel::TrySendError::Full(_)) => true,
            Err(channel::TrySendError::Disconnected(_)) => false,
        }
    }

    pub(super) fn is_live(&self) -> bool {
        !matches!(
            self.alive.try_recv(),
            Err(channel::TryRecvError::Disconnected)
        )
    }
}

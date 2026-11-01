//! Owned subscriptions. A view hands out a [`Receiver`], the pump fills its
//! [`Sink`], and a [`Scope`] filters what passes between them.

use super::Stream;
use crate::chan::oneshot;
use crate::data::{Boundary, Change, Loss, SampleBatch, StreamKey, StreamMetadataSnapshot};
use crate::device::session::Handle;
use crate::proto::{DeviceRoute, SampleNumber};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    /// Its owner closed the connection with
    /// [`Connection::close`](crate::device::Connection::close). Terminal.
    #[error("the connection was closed")]
    Closed,
    /// The time bound passed. Only the time-bounded receives report this.
    #[error("timed out waiting for an item")]
    Timeout,
}

/// An entry on a subscription's queue.
#[derive(Debug)]
pub enum Queued<T> {
    /// An item.
    Item(T),
    /// The count of items dropped while the receiver was behind.
    Gap(u64),
    /// Why the stream ended. Nothing follows.
    Ended(End),
}

/// Why a stream ended, as its last entry says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The proxy stopped.
    Stopped,
    /// Its owner closed the connection.
    Closed,
}

impl From<End> for RecvError {
    fn from(end: End) -> RecvError {
        match end {
            End::Stopped => RecvError::Stopped,
            End::Closed => RecvError::Closed,
        }
    }
}

/// An owned subscription to one subtree's stream.
///
/// Holding it keeps the connection and its stream running after every view
/// is dropped.
pub struct Receiver<T> {
    items: crate::chan::Receiver<Queued<T>>,
    /// Dropped with the receiver, which wakes the pump to drop its sink.
    _lifeline: oneshot::Sender<Infallible>,
    /// The stream this came from, which it keeps running. `None` only until
    /// [`tied_to`](Self::tied_to) sets it.
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
    /// Returns [`RecvError::Lagged`] after a gap, and [`RecvError::Stopped`],
    /// [`RecvError::Closed`] or [`RecvError::Disconnected`] once the stream
    /// has ended. Does not return
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
    /// Returns [`RecvError::Lagged`] after a gap, and [`RecvError::Stopped`],
    /// [`RecvError::Closed`] or [`RecvError::Disconnected`] once the stream
    /// has ended.
    pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
        match self.items.try_recv() {
            Ok(Queued::Item(item)) => Ok(Some(item)),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Ok(Queued::Ended(end)) => Err(end.into()),
            Err(crate::chan::TryRecvError::Empty) => Ok(None),
            Err(crate::chan::TryRecvError::Disconnected) => Err(RecvError::Disconnected),
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
    /// Returns [`RecvError::Lagged`] for a gap, [`RecvError::Stopped`] or
    /// [`RecvError::Closed`] for the end, and [`RecvError::Disconnected`] for
    /// a disconnected channel.
    pub fn resolve(
        &self,
        received: Result<Queued<T>, crate::chan::RecvError>,
    ) -> Result<T, RecvError> {
        match received {
            Ok(Queued::Item(item)) => Ok(item),
            Ok(Queued::Gap(skipped)) => Err(RecvError::Lagged(skipped)),
            Ok(Queued::Ended(end)) => Err(end.into()),
            Err(crate::chan::RecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    fn resolve_timed(
        &self,
        received: Result<Queued<T>, crate::chan::RecvTimeoutError>,
    ) -> Result<T, RecvError> {
        match received {
            Ok(item) => self.resolve(Ok(item)),
            Err(crate::chan::RecvTimeoutError::Timeout) => Err(RecvError::Timeout),
            Err(crate::chan::RecvTimeoutError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    /// Returns the underlying channel, to wait on alongside other sources
    /// with a [`flume::Selector`]. Pass what it yields to
    /// [`resolve`](Self::resolve). To await it alone, use
    /// [`recv_async`](Self::recv_async).
    pub fn as_flume(&self) -> &crate::chan::Receiver<Queued<T>> {
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
    pub(crate) route: DeviceRoute,
    pub(crate) depth: usize,
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
    pub(crate) fn covers(&self, route: DeviceRoute) -> bool {
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

/// The sending end of a queue that counts the items it had no room for. The
/// end, and an item sent with [`send_always`](Self::send_always), go in
/// however full the queue is.
///
/// A [latest-value](Self::latest) lane holds one item, and a new item takes
/// the place of one still unread instead of being counted as missed.
pub(crate) struct Lane<T> {
    items: crate::chan::Sender<Queued<T>>,
    room: usize,
    skipped: u64,
    /// A latest-value lane's own hold on its queue, through which it takes
    /// back an unread item to make room for a newer one. `None` for a FIFO
    /// lane.
    unread: Option<crate::chan::Receiver<Queued<T>>>,
}

impl<T> Lane<T> {
    /// Returns a lane with room for `capacity` items, and its receiving end.
    pub(crate) fn new(capacity: usize) -> (Lane<T>, crate::chan::Receiver<Queued<T>>) {
        let (items, received) = crate::chan::unbounded();
        let lane = Lane {
            items,
            room: capacity,
            skipped: 0,
            unread: None,
        };
        (lane, received)
    }

    /// Returns a lane that holds only the newest item, and its receiving
    /// end. Its reader never lags. One that falls behind receives the newest
    /// item next. It suits state, where each item is a whole value and the
    /// latest one is what matters.
    pub(crate) fn latest() -> (Lane<T>, crate::chan::Receiver<Queued<T>>) {
        let (mut lane, received) = Lane::new(1);
        lane.unread = Some(received.clone());
        (lane, received)
    }

    /// Sends one item, or counts it as missed if the queue is full. Returns
    /// `false` once the receiver is gone.
    pub(crate) fn send(&mut self, item: T) -> bool
    where
        T: Clone,
    {
        if let Some(unread) = &self.unread {
            // The lane's own hold keeps the queue connected, so the reader
            // is gone once that hold is the only receiver left.
            if self.items.receiver_count() <= 1 {
                return false;
            }
            // The new item goes in before the unread one comes out, in the
            // place kept for the end, so the lane never looks empty while it
            // holds an item. The pump is the only sender.
            let _ = self.items.try_send(Queued::Item(item.clone()));
            // A reader that took an item meanwhile may leave the lane empty
            // once the older one is out, and then gets the new one again.
            if self.items.len() > self.room && unread.try_recv().is_ok() && self.items.is_empty() {
                let _ = self.items.try_send(Queued::Item(item));
            }
            return true;
        }
        if !self.report_lag() {
            return false;
        }
        match self.push(Queued::Item(item)) {
            Ok(()) => true,
            Err(crate::chan::TrySendError::Full(_)) => {
                self.skipped += 1;
                true
            }
            Err(crate::chan::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Returns `true` if a FIFO lane's queue has room for one more entry.
    /// Only the lane adds entries, so an entry it sends next goes in.
    fn has_room(&self) -> bool {
        self.items.len() < self.room
    }

    /// Sends one item that is never missed, however full the queue is.
    pub(crate) fn send_always(&mut self, item: T) {
        let _ = self.items.send(Queued::Item(item));
    }

    /// Counts `missed` items that never reached the lane, and reports them
    /// as [`report_lag`](Self::report_lag) does. Returns `false` once the
    /// receiver is gone.
    pub(crate) fn lag(&mut self, missed: u64) -> bool {
        self.skipped += missed;
        self.report_lag()
    }

    /// Sends the count of missed items once the queue has room. Returns
    /// `false` once the receiver is gone.
    pub(crate) fn report_lag(&mut self) -> bool {
        if self.skipped == 0 {
            return true;
        }
        match self.push(Queued::Gap(self.skipped)) {
            Ok(()) => {
                self.skipped = 0;
                true
            }
            Err(crate::chan::TrySendError::Full(_)) => true,
            Err(crate::chan::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Ends the queue saying why.
    pub(crate) fn end(self, end: End) {
        let _ = self.items.send(Queued::Ended(end));
    }

    /// Sends `queued` if the queue has room for it.
    fn push(&self, queued: Queued<T>) -> Result<(), crate::chan::TrySendError<Queued<T>>> {
        if self.has_room() {
            self.items.try_send(queued)
        } else {
            Err(crate::chan::TrySendError::Full(queued))
        }
    }
}

/// A FIFO lane of sample batches, which carries what each batch it drops
/// changed and lost onto the next batch of that stream it queues.
///
/// For each stream it keeps the last batch it queued, and what the batches
/// it dropped since then changed and lost. A batch after drops names the
/// higher ranked of their change and its own, with the `old` values of the
/// last batch queued and the `new` values of its own, and a [`Loss`] that
/// counts every row dropped. Any other batch goes in as the parser made it.
/// The first batch of each stream the lane queues says [`Change::Initial`],
/// as its reader starts there, with a [`Loss`] that names no sample due when
/// the lane dropped the batches before it. The gaps it reports still count
/// the batches it dropped, across all streams. Each batch keeps the
/// generations the parser gave it.
pub(crate) struct BatchLane {
    lane: Lane<SampleBatch>,
    streams: HashMap<StreamKey, Carried>,
}

/// What a batch lane keeps of one stream.
#[derive(Default)]
struct Carried {
    /// The metadata and last sample number of the last batch queued.
    last: Option<(StreamMetadataSnapshot, SampleNumber)>,
    /// What the batches dropped since then changed and lost.
    pending: Option<Pending>,
}

/// What the batches of one stream that a lane dropped changed and lost.
struct Pending {
    change: Option<Change>,
    /// The rows dropped, and the rows the dropped batches said were lost.
    samples: u64,
    dropped: u64,
}

impl BatchLane {
    /// Returns a lane with room for `capacity` batches, and its receiving
    /// end.
    pub(crate) fn new(capacity: usize) -> (BatchLane, crate::chan::Receiver<Queued<SampleBatch>>) {
        let (lane, received) = Lane::new(capacity);
        let lane = BatchLane {
            lane,
            streams: HashMap::new(),
        };
        (lane, received)
    }

    /// Sends one batch, or keeps what it changed and lost for the next batch
    /// of its stream if the queue is full. Returns `false` once the receiver
    /// is gone.
    pub(crate) fn send(&mut self, batch: SampleBatch) -> bool {
        if !self.lane.report_lag() {
            return false;
        }
        let carried = self.streams.entry(batch.stream_key()).or_default();
        if !self.lane.has_room() {
            carried.miss(&batch);
            return self.lane.lag(1);
        }
        let batch = carried.queue(batch);
        self.lane.send(batch)
    }

    /// Sends the count of missed batches once the queue has room. Returns
    /// `false` once the receiver is gone.
    pub(crate) fn report_lag(&mut self) -> bool {
        self.lane.report_lag()
    }

    /// Ends the queue saying why.
    pub(crate) fn end(self, end: End) {
        self.lane.end(end);
    }
}

impl Carried {
    /// Keeps what `batch`, which the lane dropped, changed and lost.
    fn miss(&mut self, batch: &SampleBatch) {
        let pending = self.pending.get_or_insert(Pending {
            change: None,
            samples: 0,
            dropped: 0,
        });
        pending.dropped += 1;
        pending.samples += batch.len() as u64 + lost(batch);
        let change = batch.boundary().and_then(|b| b.change.clone());
        pending.change = higher(pending.change.take(), change);
    }

    /// Returns `batch` as the lane queues it, and keeps it as the last one.
    fn queue(&mut self, batch: SampleBatch) -> SampleBatch {
        let batch = match (self.pending.take(), &self.last) {
            (None, Some(_)) => batch,
            (None, None) if batch.is_initial() => batch,
            (None, None) => batch.with_boundary(Some(Boundary::of(Change::Initial))),
            (Some(pending), last) => {
                let boundary = pending.onto(&batch, last.as_ref());
                batch.with_boundary(Some(boundary))
            }
        };
        if let Some(&n) = batch.sample_numbers().last() {
            self.last = Some((batch.metadata().clone(), n));
        }
        batch
    }
}

impl Pending {
    /// Returns the boundary of `batch`, the first one queued after these
    /// drops, where `last` is the metadata and last sample number of the
    /// batch queued before them.
    fn onto(
        self,
        batch: &SampleBatch,
        last: Option<&(StreamMetadataSnapshot, SampleNumber)>,
    ) -> Boundary {
        let own = batch.boundary().and_then(|b| b.change.clone());
        let change = match last {
            Some((metadata, _)) => {
                higher(self.change, own).map(|change| restate(change, metadata, batch.metadata()))
            }
            None => Some(Change::Initial),
        };
        let expected = last
            .filter(|_| change.as_ref().is_none_or(Change::is_continuous))
            .map(|&(_, n)| SampleNumber::new(n.value() + 1));
        Boundary {
            change,
            loss: Some(Loss {
                samples: self.samples + lost(batch),
                expected,
                dropped_batches: self.dropped,
            }),
        }
    }
}

/// Returns the rows `batch` says were lost before it.
fn lost(batch: &SampleBatch) -> u64 {
    let loss = batch.boundary().and_then(|b| b.loss.as_ref());
    loss.map_or(0, |loss| loss.samples)
}

/// Returns the higher ranked of two changes a reader learns of at once. Two
/// time anomalies of one direction add up.
fn higher(first: Option<Change>, then: Option<Change>) -> Option<Change> {
    let (first, then) = match (first, then) {
        (Some(first), Some(then)) => (first, then),
        (first, then) => return first.or(then),
    };
    Some(match (first, then) {
        (Change::TimeBackward { gap_seconds: a }, Change::TimeBackward { gap_seconds: b }) => {
            Change::TimeBackward { gap_seconds: a + b }
        }
        (Change::TimeForward { gap_seconds: a }, Change::TimeForward { gap_seconds: b }) => {
            Change::TimeForward { gap_seconds: a + b }
        }
        (first, then) if then.rank() > first.rank() => then,
        (first, _) => first,
    })
}

/// Returns `change` naming the values of `old`, the metadata of the last
/// batch queued, and `new`, that of the batch it goes on.
fn restate(change: Change, old: &StreamMetadataSnapshot, new: &StreamMetadataSnapshot) -> Change {
    match change {
        Change::SessionChanged { .. } => Change::SessionChanged {
            old: old.device().session(),
            new: new.device().session(),
        },
        Change::TimeRefSessionChanged { .. } => Change::TimeRefSessionChanged {
            old: old.segment().timeref_session(),
            new: new.segment().timeref_session(),
        },
        Change::RateChanged { .. } => Change::RateChanged {
            old_rate: old.rate_hz(),
            new_rate: new.rate_hz(),
        },
        Change::Restarted { .. } => Change::Restarted {
            old_id: old.segment().segment_id(),
            new_id: new.segment().segment_id(),
        },
        Change::SegmentRollover { .. } => Change::SegmentRollover {
            old_id: old.segment().segment_id(),
            new_id: new.segment().segment_id(),
        },
        change @ (Change::Initial | Change::TimeBackward { .. } | Change::TimeForward { .. }) => {
            change
        }
    }
}

/// One subscriber's queue, a [`Lane`] or a [`BatchLane`], with its scope.
pub(crate) struct Sink<L> {
    pub(crate) lane: L,
    /// Closes when the subscriber drops its receiver, waking the pump.
    pub(crate) alive: oneshot::Receiver<Infallible>,
    pub(crate) scope: Scope,
}

impl<T> Sink<Lane<T>> {
    pub(crate) fn new(capacity: usize, scope: Scope) -> (Sink<Lane<T>>, Receiver<T>) {
        Sink::with_lane(Lane::new(capacity), scope)
    }

    /// Returns a sink whose [latest-value](Lane::latest) lane holds only the
    /// newest item, and its receiver.
    pub(crate) fn latest(scope: Scope) -> (Sink<Lane<T>>, Receiver<T>) {
        Sink::with_lane(Lane::latest(), scope)
    }

    /// Offers an item concerning `scope`, sent only if `scope` overlaps this
    /// subscriber's scope. Returns `false` once the subscriber is gone.
    pub(super) fn offer(&mut self, scope: Scope, item: impl FnOnce() -> T) -> bool
    where
        T: Clone,
    {
        !self.scope.intersects(&scope) || self.lane.send(item())
    }
}

impl Sink<BatchLane> {
    /// Returns a sink whose [`BatchLane`] has room for `capacity` batches,
    /// and its receiver.
    pub(crate) fn batches(
        capacity: usize,
        scope: Scope,
    ) -> (Sink<BatchLane>, Receiver<SampleBatch>) {
        Sink::with_lane(BatchLane::new(capacity), scope)
    }

    /// Offers a batch concerning `scope`, sent only if `scope` overlaps this
    /// subscriber's scope. Returns `false` once the subscriber is gone.
    pub(super) fn offer(&mut self, scope: Scope, batch: impl FnOnce() -> SampleBatch) -> bool {
        !self.scope.intersects(&scope) || self.lane.send(batch())
    }
}

impl<L> Sink<L> {
    fn with_lane<T>(
        (lane, items): (L, crate::chan::Receiver<Queued<T>>),
        scope: Scope,
    ) -> (Sink<L>, Receiver<T>) {
        let (lifeline, alive) = oneshot::channel();
        let receiver = Receiver {
            items,
            _lifeline: lifeline,
            stream: None,
        };
        (Sink { lane, alive, scope }, receiver)
    }
}

//! A submitted call's reply, and pipelining over many of them.

use super::error::CallError;
use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A reply that has not arrived yet.
///
/// It resolves exactly once, with the value or the error. Block on it with
/// [`wait`](Self::wait) or `.await` it. Dropping it abandons the reply, and
/// the call still goes ahead.
#[must_use = "the RPC is in flight; wait on the reply"]
#[derive(Debug)]
pub struct PendingReply {
    pub(crate) reply: flume::Receiver<Result<Vec<u8>, CallError>>,
}

impl PendingReply {
    /// Blocks until the reply resolves and returns it.
    ///
    /// # Errors
    ///
    /// Returns the call's [`CallError`]. A call whose outcome was lost
    /// resolves as [`CallError::ResponseLost`].
    pub fn wait(self) -> Result<Vec<u8>, CallError> {
        self.reply.recv().unwrap_or(Err(CallError::ResponseLost))
    }
}

impl IntoFuture for PendingReply {
    type Output = Result<Vec<u8>, CallError>;
    type IntoFuture = ReplyFuture;

    fn into_future(self) -> ReplyFuture {
        ReplyFuture(self.reply.into_recv_async())
    }
}

/// A [`PendingReply`] being awaited.
#[derive(Debug)]
pub struct ReplyFuture(flume::r#async::RecvFut<'static, Result<Vec<u8>, CallError>>);

impl Future for ReplyFuture {
    type Output = Result<Vec<u8>, CallError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|outcome| outcome.unwrap_or(Err(CallError::ResponseLost)))
    }
}

/// Returns the replies to `calls` in order, keeping up to `window` calls in
/// flight.
///
/// The iterator draws the next call from `calls` each time the window opens.
/// A lazy iterator of [`submit`](crate::device::Device::submit) calls sends
/// each call at that moment. Taking a reply blocks until it arrives. No call
/// is retransmitted.
///
/// # Panics
///
/// Panics in debug builds if `window` is zero.
pub fn pipelined(
    calls: impl IntoIterator<Item = PendingReply>,
    window: usize,
) -> impl Iterator<Item = Result<Vec<u8>, CallError>> {
    let mut calls = Pipeline::new(calls, window);
    std::iter::from_fn(move || calls.next_call().map(PendingReply::wait))
}

/// A run of calls whose replies are taken in order, with up to a window of
/// calls in flight. [`pipelined`] blocks on each reply, and
/// [`next`](Self::next) awaits it.
pub(crate) struct Pipeline<I> {
    calls: I,
    window: usize,
    in_flight: VecDeque<PendingReply>,
}

impl<I: Iterator<Item = PendingReply>> Pipeline<I> {
    /// Returns the run of `calls`, keeping up to `window` calls in flight.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `window` is zero.
    pub(crate) fn new(calls: impl IntoIterator<IntoIter = I>, window: usize) -> Pipeline<I> {
        debug_assert!(window > 0, "a zero window would never submit anything");
        Pipeline {
            calls: calls.into_iter(),
            window,
            in_flight: VecDeque::with_capacity(window),
        }
    }

    /// Draws calls from the run until the window is full, and returns the
    /// earliest call in flight. Returns `None` once every call has been
    /// taken.
    fn next_call(&mut self) -> Option<PendingReply> {
        let room = self.window - self.in_flight.len();
        self.in_flight.extend(self.calls.by_ref().take(room));
        self.in_flight.pop_front()
    }

    /// Waits for the reply to the earliest call in flight, as
    /// [`next_call`](Self::next_call) draws it. Returns `None` once every
    /// call has been answered.
    pub(crate) async fn next(&mut self) -> Option<Result<Vec<u8>, CallError>> {
        Some(self.next_call()?.await)
    }
}

/// Polls `future` once with a waker that does nothing.
/// A future over replies already resolved is ready at once.
#[cfg(test)]
pub(crate) fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[cfg(test)]
pub(crate) fn resolved(reply: Option<Vec<u8>>) -> PendingReply {
    let (resolve, reply_rx) = flume::bounded(1);
    if let Some(reply) = reply {
        resolve.send(Ok(reply)).expect("the receiver is held");
    }
    PendingReply { reply: reply_rx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn replies_come_back_in_submission_order_a_window_ahead() {
        let submitted = Cell::new(0);
        let calls = (0u8..10).map(|i| {
            submitted.set(submitted.get() + 1);
            resolved(Some(vec![i]))
        });
        let mut replies = pipelined(calls, 4);

        assert_eq!(replies.next().unwrap().unwrap(), [0]);
        assert_eq!(submitted.get(), 4);
        let rest: Vec<Vec<u8>> = replies.map(Result::unwrap).collect();
        assert_eq!(rest, (1u8..10).map(|i| vec![i]).collect::<Vec<_>>());
        assert_eq!(submitted.get(), 10);
    }

    #[test]
    fn an_abandoned_reply_resolves_as_lost() {
        assert!(matches!(
            resolved(None).wait(),
            Err(CallError::ResponseLost)
        ));
    }

    #[test]
    fn a_pending_reply_can_be_awaited() {
        let (resolve, reply) = flume::bounded(1);
        let pending = PendingReply { reply };
        let resolver = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            resolve.send(Ok(b"later".to_vec())).unwrap();
        });

        let mut awaited = std::pin::pin!(pending.into_future());
        let reply = loop {
            if let Poll::Ready(reply) = poll_once(awaited.as_mut()) {
                break reply;
            }
            std::thread::yield_now();
        };
        assert_eq!(reply.unwrap(), b"later");
        resolver.join().unwrap();
    }
}

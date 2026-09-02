//! A submitted call's reply, and pipelining over many of them.

use super::error::CallError;
use crate::tio::proxy::{RawCallError, RawCallResult};
use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A reply that has not arrived yet. It resolves exactly once, with the value
/// or the error; block with [`wait`](Self::wait) or `.await` it. Dropping it
/// abandons the reply, not the call.
#[must_use = "the RPC is in flight; wait on the reply"]
pub struct PendingReply {
    pub(crate) reply: oneshot::Receiver<RawCallResult>,
}

impl PendingReply {
    /// Block until the reply resolves.
    pub fn wait(self) -> Result<Vec<u8>, CallError> {
        Ok(self
            .reply
            .recv()
            .unwrap_or(Err(RawCallError::ProxyClosed))?)
    }
}

impl IntoFuture for PendingReply {
    type Output = Result<Vec<u8>, CallError>;
    type IntoFuture = ReplyFuture;

    fn into_future(self) -> ReplyFuture {
        ReplyFuture(self.reply.into_future())
    }
}

/// A [`PendingReply`] being awaited.
pub struct ReplyFuture(oneshot::AsyncReceiver<RawCallResult>);

impl Future for ReplyFuture {
    type Output = Result<Vec<u8>, CallError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|outcome| Ok(outcome.unwrap_or(Err(RawCallError::ProxyClosed))?))
    }
}

/// The replies to `calls` in submission order, with up to `window` in flight.
/// Calls are submitted as the window opens; nothing is ever retransmitted.
pub fn pipelined(
    calls: impl IntoIterator<Item = PendingReply>,
    window: usize,
) -> impl Iterator<Item = Result<Vec<u8>, CallError>> {
    debug_assert!(window > 0, "a zero window would never submit anything");
    let mut calls = calls.into_iter();
    let mut in_flight = VecDeque::with_capacity(window);
    std::iter::from_fn(move || {
        in_flight.extend(calls.by_ref().take(window - in_flight.len()));
        in_flight.pop_front().map(PendingReply::wait)
    })
}

#[cfg(test)]
pub(super) fn resolved(reply: Option<Vec<u8>>) -> PendingReply {
    let (resolve, reply_rx) = oneshot::channel();
    if let Some(reply) = reply {
        resolve.send(Ok(reply)).expect("the receiver is held");
    }
    PendingReply { reply: reply_rx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::task::Waker;

    fn block_on<F: IntoFuture>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future.into_future());
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            std::thread::yield_now();
        }
    }

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
        let (resolve, reply) = oneshot::channel();
        let pending = PendingReply { reply };
        let resolver = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            resolve.send(Ok(b"later".to_vec())).unwrap();
        });

        assert_eq!(block_on(pending).unwrap(), b"later");
        resolver.join().unwrap();
    }
}

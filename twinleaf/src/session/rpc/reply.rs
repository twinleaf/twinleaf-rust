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
    pub(crate) reply: crate::chan::Receiver<Result<Vec<u8>, CallError>>,
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
pub struct ReplyFuture(crate::chan::RecvFut<'static, Result<Vec<u8>, CallError>>);

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

/// The most reads in flight at once to one device while the host reads many
/// entries in a row.
pub const READ_WINDOW: usize = 4;

/// The most times a [`ReadPipeline`] asks for one read.
const READ_ASKS: u8 = 3;

/// A run of calls whose replies are taken in order, with up to a window of
/// calls in flight. [`pipelined`] blocks on each reply.
struct Pipeline<I> {
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
    fn new(calls: impl IntoIterator<IntoIter = I>, window: usize) -> Pipeline<I> {
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
}

/// A run of reads whose replies are awaited in order, which asks again for a
/// read that times out.
///
/// Each read is a key, and `submit` sends the call for a key. Until a read
/// times out, up to a window of reads are in flight. From the first
/// [`CallError::Timeout`] on, the run goes one at a time. It draws no new
/// read until every read already in flight has resolved, and keeps the
/// reply of each one that answers. Then it asks again for each read that
/// timed out, in order, one at a time, and after them draws the remaining
/// reads one at a time. A read is asked at most three times, and its third
/// timeout is its reply. Any other error is the read's reply at once, is
/// not asked again, and leaves the window as it is. Replies come back in
/// the order of the keys.
pub(crate) struct ReadPipeline<I: Iterator, S> {
    keys: I,
    submit: S,
    window: usize,
    one_at_a_time: bool,
    reads: VecDeque<Read<I::Item>>,
}

/// One read of a [`ReadPipeline`], with the times it was asked.
struct Read<K> {
    key: K,
    asks: u8,
    reply: ReadReply,
}

/// A read's reply, still in flight or resolved.
enum ReadReply {
    InFlight(ReplyFuture),
    Resolved(Result<Vec<u8>, CallError>),
}

impl<I: Iterator, S: FnMut(&I::Item) -> PendingReply> ReadPipeline<I, S> {
    /// Returns the run of reads of `keys` through `submit`, keeping up to
    /// `window` reads in flight.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `window` is zero.
    pub(crate) fn new(
        keys: impl IntoIterator<IntoIter = I>,
        submit: S,
        window: usize,
    ) -> ReadPipeline<I, S> {
        debug_assert!(window > 0, "a zero window would never submit anything");
        ReadPipeline {
            keys: keys.into_iter(),
            submit,
            window,
            one_at_a_time: false,
            reads: VecDeque::with_capacity(window),
        }
    }

    /// Waits for the reply to the earliest read not yet taken. Returns
    /// `None` once every read has been answered.
    pub(crate) async fn next(&mut self) -> Option<Result<Vec<u8>, CallError>> {
        loop {
            let room = if self.one_at_a_time {
                usize::from(self.reads.is_empty())
            } else {
                self.window - self.reads.len()
            };
            for _ in 0..room {
                let Some(key) = self.keys.next() else { break };
                let reply = ReadReply::InFlight((self.submit)(&key).into_future());
                self.reads.push_back(Read {
                    key,
                    asks: 1,
                    reply,
                });
            }
            let mut read = self.reads.pop_front()?;
            let outcome = match read.reply {
                ReadReply::InFlight(reply) => reply.await,
                ReadReply::Resolved(outcome) => outcome,
            };
            match outcome {
                Err(CallError::Timeout) if read.asks < READ_ASKS => {
                    if !self.one_at_a_time {
                        self.one_at_a_time = true;
                        self.resolve_in_flight().await;
                    }
                    read.asks += 1;
                    read.reply = ReadReply::InFlight((self.submit)(&read.key).into_future());
                    self.reads.push_front(read);
                }
                outcome => return Some(outcome),
            }
        }
    }

    /// Waits for every read in flight and keeps each outcome.
    async fn resolve_in_flight(&mut self) {
        for read in &mut self.reads {
            if let ReadReply::InFlight(reply) = &mut read.reply {
                read.reply = ReadReply::Resolved(reply.await);
            }
        }
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
    let (resolve, reply_rx) = crate::chan::bounded(1);
    if let Some(reply) = reply {
        resolve.send(Ok(reply)).expect("the receiver is held");
    }
    PendingReply { reply: reply_rx }
}

/// A reply that is ready at once as a [`CallError::Timeout`].
#[cfg(test)]
pub(crate) fn timed_out() -> PendingReply {
    let (resolve, reply_rx) = crate::chan::bounded(1);
    resolve
        .send(Err(CallError::Timeout))
        .expect("the receiver is held");
    PendingReply { reply: reply_rx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

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

    /// Takes the next reply of `reads`, which is ready at once.
    fn ready<I: Iterator, S: FnMut(&I::Item) -> PendingReply>(
        reads: &mut ReadPipeline<I, S>,
    ) -> Option<Result<Vec<u8>, CallError>> {
        match poll_once(std::pin::pin!(reads.next())) {
            Poll::Ready(reply) => reply,
            Poll::Pending => panic!("the reply waits on calls already resolved"),
        }
    }

    /// Returns a reply that is ready at once as `outcome`.
    fn answered(outcome: Result<Vec<u8>, CallError>) -> PendingReply {
        let (resolve, reply) = crate::chan::bounded(1);
        resolve.send(outcome).expect("the receiver is held");
        PendingReply { reply }
    }

    fn refused() -> CallError {
        CallError::DeviceError(super::super::RpcErrorPayload {
            error: crate::proto::rpc::RpcError::State,
            extra: Vec::new(),
        })
    }

    #[test]
    fn a_read_that_times_out_once_is_asked_again_in_its_place() {
        let submitted = RefCell::new(Vec::new());
        let mut reads = ReadPipeline::new(
            0u8..3,
            |key: &u8| {
                submitted.borrow_mut().push(*key);
                let asks = submitted.borrow().iter().filter(|k| *k == key).count();
                if *key == 1 && asks == 1 {
                    timed_out()
                } else {
                    resolved(Some(vec![*key]))
                }
            },
            4,
        );

        for key in 0u8..3 {
            assert_eq!(ready(&mut reads).unwrap().unwrap(), [key]);
        }
        assert!(ready(&mut reads).is_none());
        assert_eq!(*submitted.borrow(), [0, 1, 2, 1]);
    }

    #[test]
    fn after_a_timeout_reads_wait_for_the_window_then_go_one_at_a_time() {
        let submitted = RefCell::new(Vec::new());
        let held = RefCell::new(None);
        let mut reads = ReadPipeline::new(
            0u8..8,
            |key: &u8| {
                submitted.borrow_mut().push(*key);
                let asks = submitted.borrow().iter().filter(|k| *k == key).count();
                match (*key, asks) {
                    (0, 1) => timed_out(),
                    (1, _) => {
                        let (resolve, reply) = crate::chan::bounded(1);
                        *held.borrow_mut() = Some(resolve);
                        PendingReply { reply }
                    }
                    _ => resolved(Some(vec![*key])),
                }
            },
            4,
        );

        {
            let mut first = std::pin::pin!(reads.next());
            assert!(poll_once(first.as_mut()).is_pending());
            assert_eq!(
                *submitted.borrow(),
                [0, 1, 2, 3],
                "asked before the window cleared"
            );
            let resolve = held.borrow_mut().take().expect("key 1 is in flight");
            resolve.send(Ok(vec![1])).unwrap();
            let Poll::Ready(Some(Ok(reply))) = poll_once(first.as_mut()) else {
                panic!("the window has cleared");
            };
            assert_eq!(reply, [0]);
        }
        assert_eq!(*submitted.borrow(), [0, 1, 2, 3, 0]);
        for key in 1u8..4 {
            assert_eq!(ready(&mut reads).unwrap().unwrap(), [key]);
            assert_eq!(submitted.borrow().len(), 5, "a call was drawn early");
        }
        for key in 4u8..8 {
            assert_eq!(ready(&mut reads).unwrap().unwrap(), [key]);
            assert_eq!(submitted.borrow().len(), 2 + usize::from(key));
        }
        assert!(ready(&mut reads).is_none());
    }

    #[test]
    fn a_read_that_answered_while_another_timed_out_is_not_asked_again() {
        let submitted = RefCell::new(Vec::new());
        let mut reads = ReadPipeline::new(
            0u8..4,
            |key: &u8| {
                submitted.borrow_mut().push(*key);
                let asks = submitted.borrow().iter().filter(|k| *k == key).count();
                if key.is_multiple_of(2) && asks == 1 {
                    timed_out()
                } else {
                    resolved(Some(vec![*key]))
                }
            },
            4,
        );

        for key in 0u8..4 {
            assert_eq!(ready(&mut reads).unwrap().unwrap(), [key]);
        }
        assert_eq!(*submitted.borrow(), [0, 1, 2, 3, 0, 2]);
    }

    #[test]
    fn a_read_that_times_out_three_times_returns_the_timeout() {
        let submitted = RefCell::new(Vec::new());
        let mut reads = ReadPipeline::new(
            0u8..2,
            |key: &u8| {
                submitted.borrow_mut().push(*key);
                if *key == 0 {
                    timed_out()
                } else {
                    resolved(Some(vec![*key]))
                }
            },
            4,
        );

        assert!(matches!(ready(&mut reads), Some(Err(CallError::Timeout))));
        assert_eq!(ready(&mut reads).unwrap().unwrap(), [1]);
        assert!(ready(&mut reads).is_none());
        assert_eq!(*submitted.borrow(), [0, 1, 0, 0]);
    }

    #[test]
    fn a_refused_read_returns_at_once_and_keeps_the_window() {
        let submitted = RefCell::new(Vec::new());
        let mut reads = ReadPipeline::new(
            0u8..6,
            |key: &u8| {
                submitted.borrow_mut().push(*key);
                match key {
                    0 => answered(Err(refused())),
                    _ => resolved(Some(vec![*key])),
                }
            },
            4,
        );

        assert!(matches!(
            ready(&mut reads),
            Some(Err(CallError::DeviceError(_)))
        ));
        assert_eq!(*submitted.borrow(), [0, 1, 2, 3]);
        assert_eq!(ready(&mut reads).unwrap().unwrap(), [1]);
        assert_eq!(*submitted.borrow(), [0, 1, 2, 3, 4], "the window stays");
        for key in 2u8..6 {
            assert_eq!(ready(&mut reads).unwrap().unwrap(), [key]);
        }
        assert_eq!(*submitted.borrow(), [0, 1, 2, 3, 4, 5]);
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
        let (resolve, reply) = crate::chan::bounded(1);
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

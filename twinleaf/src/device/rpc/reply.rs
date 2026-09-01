//! A submitted call's reply, and pipelining over many of them.

use super::error::CallError;
use crate::tio::proxy::{RawCallError, RawCallResult};
use crossbeam::channel;
use std::collections::VecDeque;

/// A reply that has not arrived yet.
///
/// Every pending reply resolves: with the value, or with the device's or the
/// proxy's error (the proxy times outstanding calls out). Dropping it abandons
/// the reply; the call itself still runs.
#[must_use = "the RPC is in flight; wait on the reply"]
pub struct PendingReply {
    pub(crate) replies: channel::Receiver<RawCallResult>,
}

impl PendingReply {
    /// Block until the reply resolves.
    pub fn wait(self) -> Result<Vec<u8>, CallError> {
        Ok(self
            .replies
            .recv()
            .unwrap_or(Err(RawCallError::ProxyClosed))?)
    }
}

/// The replies to `calls`, in submission order, keeping up to `window` calls in
/// flight to hide round trips.
///
/// Calls are submitted lazily as the window opens. The first call that fails
/// to submit ends the sequence with its error, abandoning the replies still in
/// flight. Nothing is ever retransmitted: only the caller knows whether a
/// repeated call is harmless.
pub fn pipelined<E: From<CallError>>(
    calls: impl IntoIterator<Item = Result<PendingReply, E>>,
    mut window: usize,
) -> impl Iterator<Item = Result<Vec<u8>, E>> {
    debug_assert!(window > 0, "a zero window would never submit anything");
    let mut calls = calls.into_iter();
    let mut in_flight = VecDeque::with_capacity(window);
    std::iter::from_fn(move || {
        while in_flight.len() < window {
            match calls.next() {
                Some(Ok(pending)) => in_flight.push_back(pending),
                Some(Err(error)) => {
                    window = 0;
                    in_flight.clear();
                    return Some(Err(error));
                }
                None => break,
            }
        }
        in_flight
            .pop_front()
            .map(|pending| pending.wait().map_err(E::from))
    })
}

#[cfg(test)]
pub(super) fn resolved(reply: Option<Vec<u8>>) -> PendingReply {
    let (result, replies) = channel::bounded(1);
    if let Some(reply) = reply {
        result.send(Ok(reply)).expect("the receiver is held");
    }
    PendingReply { replies }
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
            Ok::<_, CallError>(resolved(Some(vec![i])))
        });
        let mut replies = pipelined(calls, 4);

        assert_eq!(replies.next().unwrap().unwrap(), [0]);
        assert_eq!(submitted.get(), 4);
        let rest: Vec<Vec<u8>> = replies.map(Result::unwrap).collect();
        assert_eq!(rest, (1u8..10).map(|i| vec![i]).collect::<Vec<_>>());
        assert_eq!(submitted.get(), 10);
    }

    #[test]
    fn a_failed_submit_ends_the_sequence_with_its_error() {
        let calls = [
            Ok(resolved(Some(Vec::new()))),
            Err(CallError::RequestNotSubmitted),
            Ok(resolved(Some(Vec::new()))),
        ];
        let mut replies = pipelined(calls, 2);

        assert!(matches!(
            replies.next(),
            Some(Err(CallError::RequestNotSubmitted))
        ));
        assert!(replies.next().is_none());
    }

    #[test]
    fn an_abandoned_reply_resolves_as_lost() {
        assert!(matches!(
            resolved(None).wait(),
            Err(CallError::ResponseLost)
        ));
    }
}

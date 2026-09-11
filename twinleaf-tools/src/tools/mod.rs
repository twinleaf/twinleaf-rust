pub mod capture;
pub mod dump;
pub mod health;
pub mod log;
pub mod monitor;
pub mod proxy;
pub mod rpc;
pub mod simulate;
pub mod upgrade;

use std::time::Instant;
use twinleaf::device::RecvError;
use twinleaf::Receiver;

/// Blocks for the next item, returning `None` once `deadline` passes. Lag is
/// reported and skipped: what a subscription shed is not the end of it.
pub fn recv_before<T>(
    items: &Receiver<T>,
    deadline: Option<Instant>,
    kind: &str,
) -> eyre::Result<Option<T>> {
    loop {
        let next = match deadline {
            Some(deadline) => items.recv_deadline(deadline),
            None => items.recv(),
        };
        return match next {
            Ok(item) => Ok(Some(item)),
            Err(RecvError::Lagged(skipped)) => {
                ::log::warn!("dropped {skipped} {kind}");
                continue;
            }
            Err(RecvError::Timeout) => Ok(None),
            Err(error @ RecvError::Disconnected) => Err(eyre::Report::new(error)),
        };
    }
}

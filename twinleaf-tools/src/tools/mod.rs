use std::time::Instant;
use twinleaf::tio::{self, proxy as tio_proxy};

pub mod capture;
pub mod dump;
pub mod health;
pub mod log;
pub mod monitor;
pub mod proxy;
pub mod rpc;
pub mod simulate;
pub mod upgrade;

/// Blocks for the next packet, returning `None` once the deadline expires.
pub fn recv_before(
    port: &tio_proxy::Port,
    deadline: Option<Instant>,
) -> Result<Option<tio::Packet>, tio_proxy::RecvError> {
    match deadline {
        None => port.recv().map(Some),
        Some(deadline) if Instant::now() >= deadline => Ok(None),
        Some(deadline) => match port.recv_deadline(deadline) {
            Ok(pkt) => Ok(Some(pkt)),
            Err(tio_proxy::RecvTimeoutError::Timeout) => Ok(None),
            Err(tio_proxy::RecvTimeoutError::ProxyDisconnected) => {
                Err(tio_proxy::RecvError::ProxyDisconnected)
            }
        },
    }
}

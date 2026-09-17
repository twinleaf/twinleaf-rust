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
use twinleaf::proto::log::LogMessage;
use twinleaf::Receiver;

/// One device log line: its text, and the data word a device sends with a
/// constant message when it has a count, a second, or a session to report.
pub fn log_line(message: &LogMessage<'_>) -> String {
    let text = String::from_utf8_lossy(message.message);
    match message.data {
        0 => text.into_owned(),
        data => format!("{text} {data}"),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf::proto::log::LogLevel;

    #[test]
    fn a_log_line_shows_the_data_word_a_device_sent() {
        let line = |data| {
            log_line(&LogMessage {
                level: LogLevel::INFO,
                data,
                message: b"samples dropped",
            })
        };
        assert_eq!(line(0), "samples dropped");
        assert_eq!(line(42), "samples dropped 42");
    }
}

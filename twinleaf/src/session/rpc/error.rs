use super::RpcValueDecodeError;
use crate::proto::rpc as wire;
use crate::proto::RouteError;

/// A device's refusal of an RPC, with its error code and message.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}{}", said(.extra))]
pub struct RpcErrorPayload {
    /// The code.
    pub error: wire::RpcError,
    /// The message bytes, if the device sent any.
    pub extra: Vec<u8>,
}

impl RpcErrorPayload {
    /// Creates an owned payload from a parsed error reply.
    pub fn from_wire(error: wire::ErrorReply<'_>) -> Self {
        Self {
            error: error.error(),
            extra: error.message.to_vec(),
        }
    }
}

/// The message a device sent after its code, behind a colon, or nothing.
fn said(extra: &[u8]) -> String {
    match extra {
        [] => String::new(),
        message => format!(": {}", String::from_utf8_lossy(message)),
    }
}

/// Why an RPC call produced no value.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum CallError {
    /// The request was not sent. The socket's queue was full, or the name
    /// did not fit a packet.
    #[error("RPC request was not submitted to the proxy")]
    RequestNotSubmitted,
    /// The route lies outside this view.
    #[error("RPC route outside this view: {0}")]
    InvalidRoute(#[from] RouteError),
    /// The connection to the proxy ended before the reply came.
    #[error("proxy disconnected while waiting for the RPC reply")]
    ResponseLost,
    /// No reply within the view's RPC timeout.
    #[error("timed out waiting for the RPC reply")]
    Timeout,
    /// The device refused the call.
    #[error("device returned error: {0}")]
    DeviceError(RpcErrorPayload),
    /// The reply did not decode as the type expected of it. That type is
    /// the caller's own or the one the device's RPC table gives.
    #[error(transparent)]
    InvalidReply(RpcValueDecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refusal shows the message a device sent after its code.
    #[test]
    fn a_refusal_shows_its_message() {
        let refused = |extra: &[u8]| RpcErrorPayload {
            error: wire::RpcError::State,
            extra: extra.to_vec(),
        };
        assert_eq!(
            refused(b"privileged: unlock with dev.priv").to_string(),
            "wrong device state: privileged: unlock with dev.priv"
        );
        assert_eq!(refused(b"").to_string(), "wrong device state");
    }

    /// A reply too short for its type reads as the reason it did not decode.
    #[test]
    fn a_short_reply_reads_as_its_reason() {
        let short = crate::tio::rpc::RpcDecodeError::TooShort {
            expected: 4,
            actual: 2,
        };
        assert_eq!(
            CallError::InvalidReply(short.into()).to_string(),
            "RPC reply is too short: expected at least 4 bytes, got 2"
        );
    }
}

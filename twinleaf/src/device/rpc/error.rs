use super::codec::RpcDecodeError;
use crate::proto::rpc as wire;
use crate::proto::RouteError;
use crate::tio::proxy;

/// A device's refusal, owned so a client can carry it out of the receive loop.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}")]
pub struct RpcErrorPayload {
    /// The code.
    pub error: wire::RpcError,
    /// The message bytes, if the device sent any.
    pub extra: Vec<u8>,
}

impl RpcErrorPayload {
    /// Own a parsed error reply.
    pub fn from_wire(error: wire::ErrorReply<'_>) -> Self {
        Self {
            error: error.error(),
            extra: error.message.to_vec(),
        }
    }
}

/// Why one RPC call did not produce a value: it was never submitted, no reply
/// came back, the device refused, or the reply did not decode.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CallError {
    /// Never sent: the proxy was busy, or the name did not fit a packet.
    #[error("RPC request was not submitted to the proxy")]
    RequestNotSubmitted,
    /// The route lies outside this view.
    #[error("RPC route outside this view: {0}")]
    InvalidRoute(#[from] RouteError),
    /// The proxy closed before the reply came.
    #[error("proxy disconnected while waiting for the RPC reply")]
    ResponseLost,
    /// No reply within the view's RPC timeout.
    #[error("timed out waiting for the RPC reply")]
    Timeout,
    /// The device left before answering.
    #[error("device disconnected before the RPC completed")]
    DeviceDisconnected,
    /// The device refused the call.
    #[error("device returned error: {0}")]
    DeviceError(RpcErrorPayload),
    /// The reply did not decode as the expected type.
    #[error("RPC reply did not match expected type: {0}")]
    InvalidReply(#[source] RpcDecodeError),
}

impl From<proxy::RawCallError> for CallError {
    fn from(error: proxy::RawCallError) -> Self {
        match error {
            proxy::RawCallError::InvalidRoute(error) => Self::InvalidRoute(error),
            proxy::RawCallError::RequestNotSubmitted => Self::RequestNotSubmitted,
            proxy::RawCallError::Timeout => Self::Timeout,
            proxy::RawCallError::DeviceDisconnected => Self::DeviceDisconnected,
            proxy::RawCallError::ProxyClosed => Self::ResponseLost,
            proxy::RawCallError::Device { error, message } => Self::DeviceError(RpcErrorPayload {
                error,
                extra: message,
            }),
        }
    }
}

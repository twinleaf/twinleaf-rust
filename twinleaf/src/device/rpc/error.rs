use super::RpcDecodeError;
use crate::tio::proxy;

use twinleaf_proto::rpc as wire;

/// A device's refusal, owned so a client can carry it out of the receive loop.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}")]
pub struct RpcErrorPayload {
    pub error: wire::RpcError,
    pub extra: Vec<u8>,
}

impl RpcErrorPayload {
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
    #[error("RPC request was not submitted to the proxy")]
    RequestNotSubmitted,
    #[error("RPC route outside this view: {0}")]
    InvalidRoute(#[from] twinleaf_proto::RouteError),
    #[error("proxy disconnected while waiting for the RPC reply")]
    ResponseLost,
    #[error("timed out waiting for the RPC reply")]
    Timeout,
    #[error("device disconnected before the RPC completed")]
    DeviceDisconnected,
    #[error("device returned error: {0}")]
    DeviceError(RpcErrorPayload),
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

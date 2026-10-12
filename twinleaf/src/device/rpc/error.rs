use crate::proto::rpc as wire;
use crate::proto::RouteError;
use crate::tio::rpc::RpcDecodeError;

/// A device's refusal of an RPC, with its error code and message.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}")]
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
    /// The reply did not decode as the expected type.
    #[error("RPC reply did not match expected type: {0}")]
    InvalidReply(#[source] RpcDecodeError),
}

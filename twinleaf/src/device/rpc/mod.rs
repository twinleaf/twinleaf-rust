//! The host's RPC vocabulary: typed and raw call encoding, the errors a call
//! can fail with, a submitted call's [`PendingReply`], and discovery
//! ([`RpcRegistry`]) layered over a call surface.

mod cache;
mod codec;
mod error;
mod registry;
mod reply;
mod value;

pub use crate::tio::proto::RpcMethod;
pub use codec::{RpcArgs, RpcDecodeError, RpcReply, RpcReplyFixedSize};
pub use error::{CallError, RpcErrorPayload};
pub use registry::{RpcDescriptor, RpcRegistry, RpcRegistryError};
pub use reply::{pipelined, PendingReply};
pub use twinleaf_proto::rpc::{RpcAccess, RpcMeta, RpcMetaFlags, RpcStringLen, RpcValueType};
pub use value::{RpcMetaExt, RpcValue, RpcValueDecodeError, RpcValueEncodeError, RpcValueTypeExt};

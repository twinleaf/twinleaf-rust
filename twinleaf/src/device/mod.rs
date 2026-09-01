pub mod capture;
mod connection;
pub mod discovery;
pub(crate) mod rpc;
mod stream;

pub use crate::tio::proto::route::RouteError;
pub use crate::tio::proto::DeviceRoute;
pub use connection::{Connection, Device, DeviceTree, MetadataError};
pub use rpc::{
    pipelined, CallError, PendingReply, RpcAccess, RpcArgs, RpcDecodeError, RpcDescriptor,
    RpcErrorPayload, RpcMeta, RpcMetaExt, RpcMetaFlags, RpcMethod, RpcRegistry, RpcRegistryError,
    RpcReply, RpcReplyFixedSize, RpcStringLen, RpcValue, RpcValueDecodeError, RpcValueEncodeError,
    RpcValueType, RpcValueTypeExt,
};
pub use stream::{DeviceEvent, Event, LinkEvent, NamedRoute, Receiver, RecvError, TreeEvent};

pub mod capture;
mod connection;
pub mod discovery;
mod rpc;

pub use crate::tio::proto::DeviceRoute;
pub use capture::CaptureRpc;
pub use connection::{
    BatchError, Device, DeviceEvent, DeviceTree, NamedRoute, ProxyDisconnected, TreeEvent, TreeItem,
};
pub use rpc::{RpcClient, RpcDescriptor, RpcRegistry, RpcRegistryError};

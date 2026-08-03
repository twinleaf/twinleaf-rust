mod client;
mod registry;

pub use client::{RpcClient, RpcList, RpcListError};
pub use registry::{RpcDescriptor, RpcRegistry};

//! Calling a device's RPCs from the host.
//!
//! One may call an RPC on two paths, depending on what is known at compile time:
//!
//! - (**Typed**) The RPC's name and Rust types are known
//!     - [`RpcArgs`] encodes the arguments
//!     - [`RpcReply`] decodes the reply, for scalars, strings, byte vectors, and tuples of those.
//!     - [`rpc`](crate::device::Device::rpc), [`get`](crate::device::Device::get), and
//!       [`action`](crate::device::Device::action) are direct calls using the **typed** system.
//! - (**Dynamic**) The RPC is learned from the device
//!     - [`RpcRegistry`] walks the device's table into [`RpcDescriptor`]s
//!     - [`RpcMeta`] says what type an RPC carries and whether it can be read or written.
//!     - [`RpcValueTypeExt`] then encodes and decodes an owned [`RpcValue`] of that type.
//!     - An interactive tool or a GUI may prefer this way.
//!
//! Either way, a call may be issued with [`submit`](crate::device::Device::submit)
//! and answered through a [`PendingReply`] (blocking or awaited). Using
//! [`pipelined`] instead keeps a window of them in flight. Every failure is a
//! [`CallError`] with a device's refusal carrying its [`RpcErrorPayload`].
//!
//! [`RpcMeta`], [`RpcValueType`], [`RpcAccess`], [`RpcStringLen`],
//! [`RpcMetaFlags`], and [`RpcMethod`] are the protocol's own definitions,
//! re-exported from [`twinleaf_proto::rpc`] so a host needs no second crate.
//! The wire layout of requests, replies, and errors is documented there.

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
pub use reply::{pipelined, PendingReply, ReplyFuture};
pub use twinleaf_proto::rpc::{RpcAccess, RpcMeta, RpcMetaFlags, RpcStringLen, RpcValueType};
pub use value::{RpcMetaExt, RpcValue, RpcValueDecodeError, RpcValueEncodeError, RpcValueTypeExt};

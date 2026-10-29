//! Calling a device's RPCs from the host.
//!
//! An RPC can be called in two ways, depending on what is known at compile
//! time.
//!
//! - (**Typed**) The RPC's name and Rust types are known.
//!     - [`RpcArgs`] encodes the arguments.
//!     - [`RpcReply`] decodes the reply, for scalars, strings, byte vectors,
//!       and tuples of those.
//!     - [`rpc`](crate::device::Device::rpc), [`get`](crate::device::Device::get),
//!       and [`action`](crate::device::Device::action) are typed calls.
//! - (**Dynamic**) The RPC is learned from the device.
//!     - [`RpcRegistry`] reads the device's table into [`RpcDescriptor`]s.
//!     - [`RpcMeta`] gives the type an RPC carries and whether it can be read
//!       or written.
//!     - [`RpcValueTypeExt`] encodes and decodes an owned [`RpcValue`] of that
//!       type.
//!     - Interactive tools and GUIs use this way.
//!
//! A device announces every change to a setting or report, numbered by
//! `settings.version`, which
//! [`DeviceEvent::Setting`](crate::DeviceEvent::Setting) carries.
//! [`SettingsCache`] holds the announced values, kept by those numbers and
//! the heartbeat's, and asks for a snapshot when one was missed.
//! [`STANDARD`] and [`Channel`] say what each standard entry is for, from
//! the same table the device crate answers.
//!
//! Either way, [`submit`](crate::device::Device::submit) issues a call and
//! returns a [`PendingReply`] to block on or await. [`pipelined`] keeps a
//! window of them in flight. Every failure is a [`CallError`], and a device's
//! refusal carries its [`RpcErrorPayload`].
//!
//! [`RpcMeta`], [`RpcValueType`], [`RpcAccess`], [`RpcStringLen`], and
//! [`RpcMetaFlags`] are the protocol's own definitions, re-exported from
//! [`proto::rpc`](crate::proto::rpc). The wire layout of requests, replies,
//! and errors is documented there.

mod cache;
mod error;
mod registry;
mod reply;
mod settings;
#[cfg(test)]
mod settings_sim;
mod value;

#[doc(no_inline)]
pub use crate::proto::rpc::{RpcAccess, RpcMeta, RpcMetaFlags, RpcStringLen, RpcValueType};
#[doc(inline)]
pub use crate::tio::rpc::{RpcArgs, RpcDecodeError, RpcReply, RpcReplyFixedSize};
pub use error::{CallError, RpcErrorPayload};
pub use registry::{RpcDescriptor, RpcRegistry, RpcRegistryError};
pub use reply::{pipelined, PendingReply, ReplyFuture};
pub use settings::{CacheAction, SettingsCache};
#[doc(no_inline)]
pub use twinleaf_device::rpc::{Channel, Std, STANDARD};
pub use value::{RpcMetaExt, RpcValue, RpcValueDecodeError, RpcValueEncodeError, RpcValueTypeExt};

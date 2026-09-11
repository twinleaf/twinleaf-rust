//! Live access to a device tree over one `tio` link.
//!
//! A [`Connection`] owns the link and the workers behind it and has an API
//! to view the link at some scope:
//!
//! ```text
//! Connection                 scope: every route on the link
//! ├── DeviceTree at /1       scope: /1 and its descendants, RPCs require specifying a route
//! │   └── Device at /1/2     scope: /1/2 only, RPCs are bound to route /1/2
//! └── Device at /            scope: / only (the root device)
//! ```
//!
//! The underlying link already interleaves multiple data streams, so views have been designed to be
//! cheap copies, only validating routes for obvious errors on creation. [`Connection::tree`] and
//! [`Connection::device`] cannot fail (since the connection covers every route), but
//! narrowing a scope with [`DeviceTree::device`] or [`DeviceTree::to_depth`] is
//! checked (since no view may widen its own scope).
//!
//! Each view offers three subscriptions, filtered to its appropriate scope:
//!
//! - [`samples`](DeviceTree::samples): decoded
//!   [`SampleBatch`](crate::data::SampleBatch)es, the output of
//!   [`data::PacketParser`](crate::data::PacketParser).
//! - [`events`](DeviceTree::events): interpreted [`Event`]s, for link status,
//!   route discovery, and per-device facts such as metadata and heartbeats.
//! - [`packets`](DeviceTree::packets): the routed packets themselves
//!
//!
//! Each returns a [`Receiver`] whose [`RecvError`] tells lag, a timeout, and
//! disconnection apart.
//!
//! RPCs block by default: [`get`](Device::get), [`rpc`](Device::rpc), and
//! [`action`](Device::action) encode through the [`rpc`] module's
//! [`RpcArgs`](rpc::RpcArgs) and [`RpcReply`](rpc::RpcReply).
//! [`submit`](Device::submit) returns a [`PendingReply`](rpc::PendingReply) to
//! wait on or `.await`, and [`rpc::pipelined`] keeps a window of them in
//! flight. The RPCs for a device can be listed with
//! [`rpc_registry`](Device::rpc_registry).
//!
//! Beyond the views:
//!
//! - [`discovery`] finds devices on serial ports and, with the `mdns` feature,
//!   on the network.
//! - [`capture`] triggers and reads out a device-side buffer capture through its
//!   array RPC.
//! - [`firmware`](crate::firmware) queries and updates firmware over a
//!   [`Device`].
//!
//! # Examples
//!
//! Name every device behind a hub, then follow the link's status:
//!
//! ```no_run
//! use std::time::Duration;
//! use twinleaf::{Connection, DeviceRoute, Event, LinkEvent};
//!
//! let connection = Connection::open("serial:///dev/ttyACM0").expect("open the hub");
//! let tree = connection.tree(DeviceRoute::root());
//! for named in tree.named_routes(Duration::from_secs(2)) {
//!     println!("{}: {}", named.route, named.name.as_deref().unwrap_or("(no answer)"));
//! }
//!
//! let events = connection.events();
//! while let Ok(event) = events.recv() {
//!     if let Event::Link { subtree, event: LinkEvent::Status(status) } = event {
//!         println!("{subtree}: {status:?}");
//!     }
//! }
//! ```
//!
//! Issue several reads at once and take the replies in order:
//!
//! ```no_run
//! use twinleaf::device::rpc::{pipelined, RpcReply};
//! use twinleaf::{Connection, DeviceRoute};
//!
//! let connection = Connection::connect().expect("connect to a device");
//! let device = connection.device(DeviceRoute::root());
//! let calls = ["dev.name", "dev.desc", "dev.serial"].map(|name| device.submit(name, &[]));
//! for reply in pipelined(calls, 3) {
//!     let text = String::decode_reply(&reply.expect("a reply")).expect("a string");
//!     println!("{text}");
//! }
//! ```

pub mod capture;
mod connection;
pub mod discovery;
pub mod rpc;
pub mod runtime;
mod stream;

#[doc(no_inline)]
pub use crate::proto::RouteError;
pub use connection::{Connection, Device, DeviceTree, MetadataError};
pub use stream::{DeviceEvent, Event, LinkEvent, NamedRoute, Receiver, RecvError, TreeEvent};

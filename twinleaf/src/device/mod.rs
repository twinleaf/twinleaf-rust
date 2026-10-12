//! Live access to a device tree over one link to the machine's
//! [proxy](crate::proxy), which opens each device and shares it with every
//! process.
//!
//! A [`Connection`] owns the link and the tasks behind it. It hands out
//! views of the link, each at some scope, as the tree below shows.
//!
//! ```text
//! Connection                 scope: every route on the link
//! ├── DeviceTree at /1       scope: /1 and its descendants, RPCs take a route
//! │   └── Device at /1/2     scope: /1/2 only, RPCs go to route /1/2
//! └── Device at /            scope: / only (the root device)
//! ```
//!
//! Views are cheap to create and clone, and all of them share the
//! connection's one stream. [`Connection::tree`] and [`Connection::device`]
//! cannot fail. [`DeviceTree::device`] checks that its route lies in the
//! view, and [`DeviceTree::to_depth`] only narrows. No view reaches beyond
//! its own scope.
//!
//! Each view offers three subscriptions, filtered to its scope.
//!
//! - [`samples`](DeviceTree::samples) gives decoded
//!   [`SampleBatch`](crate::data::SampleBatch)es, the output of
//!   [`data::PacketParser`](crate::data::PacketParser).
//! - [`events`](DeviceTree::events) gives [`Event`]s for link status, route
//!   discovery, and per-device facts such as metadata and heartbeats.
//! - [`packets`](DeviceTree::packets) gives the routed packets themselves.
//!
//! Each returns a [`Receiver`]. Its [`RecvError`] tells lag, a timeout, the
//! proxy stopping, and any other end apart.
//!
//! The RPCs [`get`](Device::get), [`rpc`](Device::rpc), and
//! [`action`](Device::action) block until the reply arrives. They encode
//! through the [`rpc`] module's [`RpcArgs`](rpc::RpcArgs) and
//! [`RpcReply`](rpc::RpcReply). [`submit`](Device::submit) returns a
//! [`PendingReply`](rpc::PendingReply) to wait on or `.await`, and
//! [`rpc::pipelined`] keeps a window of them in flight.
//! [`rpc_registry`](Device::rpc_registry) lists a device's RPCs.
//!
//! The other modules build on the views.
//!
//! - [`discovery`] finds devices on serial ports and, with the `mdns` feature,
//!   on the network.
//!
//! # Examples
//!
//! Name every device behind a hub, then follow the link's status until the
//! proxy stops.
//!
//! ```no_run
//! use std::time::Duration;
//! use twinleaf::{Connection, DeviceRoute, Event, LinkEvent};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let connection = Connection::open("serial:///dev/ttyACM0")?;
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
//! # Ok(())
//! # }
//! ```
//!
//! Issue several reads at once and take the replies in order.
//!
//! ```no_run
//! use twinleaf::device::rpc::{pipelined, RpcReply};
//! use twinleaf::{Connection, DeviceRoute};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let connection = Connection::connect()?;
//! let device = connection.device(DeviceRoute::root());
//! let calls = ["dev.name", "dev.desc", "dev.serial"].map(|name| device.submit(name, &[]));
//! for reply in pipelined(calls, 3) {
//!     let text = String::decode_reply(&reply?)?;
//!     println!("{text}");
//! }
//! # Ok(())
//! # }
//! ```

mod connection;
pub mod discovery;
pub mod rpc;
pub(crate) mod runtime;
mod session;
mod stream;

#[cfg(test)]
mod fixtures;

#[doc(no_inline)]
pub use crate::proto::RouteError;
pub use connection::{Connection, Device, DeviceTree, MetadataError};
pub use stream::{DeviceEvent, Event, LinkEvent, NamedRoute, Receiver, RecvError};

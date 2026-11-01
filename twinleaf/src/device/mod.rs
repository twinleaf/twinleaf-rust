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
//! Each view offers two subscriptions, filtered to its scope.
//!
//! - [`samples`](DeviceTree::samples) gives decoded
//!   [`SampleBatch`](crate::data::SampleBatch)es, the output of
//!   [`data::PacketParser`](crate::data::PacketParser).
//! - [`packets`](DeviceTree::packets) gives the routed packets themselves.
//!
//! Each returns a [`Receiver`]. Its [`RecvError`] tells lag, a timeout, the
//! proxy stopping, and any other end apart.
//!
//! The connection also keeps each device's state current, and every view
//! shares it. [`presence`](Device::presence) says whether the device is
//! there, [`metadata`](Device::metadata) describes its streams,
//! [`rpc_registry`](Device::rpc_registry) lists its RPCs, and
//! [`settings`](Device::settings) holds the values it announces. Each returns
//! the value now, and its `_updates` method a [`Receiver`] that yields the
//! value now and then each new one. Such a receiver never lags, as a reader
//! that falls behind skips to the newest value.
//! [`routes`](DeviceTree::routes) lists the devices a tree has heard.
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
//! - [`capture`] triggers and reads out a device's buffer capture through its
//!   array RPC.
//! - [`record`] writes a tree to a log file that the [`data`](crate::data)
//!   reader decodes.
//! - [`firmware`](crate::firmware) queries and updates firmware over a
//!   [`Device`].
//!
//! # Examples
//!
//! Name every device behind a hub, then follow whether the root device is
//! there until the proxy stops.
//!
//! ```no_run
//! use std::time::Duration;
//! use twinleaf::{Connection, DeviceRoute};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let connection = Connection::open("serial:///dev/ttyACM0")?;
//! let tree = connection.tree(DeviceRoute::root());
//! for named in tree.named_routes(Duration::from_secs(2)) {
//!     println!("{}: {}", named.route, named.name.as_deref().unwrap_or("(no answer)"));
//! }
//!
//! let presence = connection.device(DeviceRoute::root()).presence_updates();
//! while let Ok(presence) = presence.recv() {
//!     println!("{presence:?}");
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

pub mod capture;
mod connection;
pub mod discovery;
pub mod record;
pub mod rpc;
pub(crate) mod runtime;
pub(crate) mod session;
mod state;
pub(crate) mod store;
pub(crate) mod stream;

#[cfg(test)]
pub(crate) mod fixtures;

#[doc(no_inline)]
pub use crate::proto::RouteError;
pub use connection::{Connection, Device, DeviceTree, MetadataError, NamedRoute};
pub use state::{
    MetadataState, Presence, RegistryState, SettingChange, SettingValueError, Settings,
};
pub use stream::{Receiver, RecvError};

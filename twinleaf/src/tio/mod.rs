//! The raw TIO packet layer: transports, the multiplexing proxy, and the wire
//! codec. This is wire and proxy-server plumbing, not the application story:
//! reach for it only to forge packets or to serve a proxy. Applications open a
//! [`Connection`](crate::Connection), whose views hand out
//! [`Packet`]s, events and samples already filtered to what they cover.

pub(crate) mod os;
pub mod packet;
pub mod proxy;
pub(crate) mod proxy_core;
pub mod transport;

pub use packet::Packet;

//! The raw TIO packet layer: transports, the multiplexing proxy, and the wire
//! codec. Reach for it only to see or forge packets; everything else belongs to
//! [`Connection`](proxy::Connection) and [`DeviceTree`](crate::DeviceTree).

pub(crate) mod os;
pub mod proto;
pub mod proxy;
pub(crate) mod proxy_core;
pub mod transport;

pub use proto::Packet;

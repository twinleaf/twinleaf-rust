//! Moving packets: the transports a host opens, the proxy that shares one
//! link among many clients, and the [`Packet`] they exchange.
//!
//! [`proto`](crate::proto) says what a packet means. This module moves
//! packets. Its types own their bytes, run threads, and reconnect. Nothing
//! here decodes samples or issues RPCs. That is [`device`](crate::device),
//! where an application starts.
//!
//! - [`packet`]: [`Packet`], one validated wire buffer read through the proto
//!   codecs, with what a host adds on its own: [`Payload`](packet::Payload),
//!   [`ProxyStatus`](packet::ProxyStatus), and an owned
//!   [`RpcMethod`](packet::RpcMethod).
//! - [`transport`]: a [`Port`](transport::Port) owns one serial, TCP, or UDP
//!   link on its own thread and reconnects.
//! - [`proxy`]: one link shared by many [`Port`](proxy::Port)s, each scoped to
//!   a subtree, with RPC ids remapped and timed out. A proxy server serves
//!   these to other processes. [`Connection`](crate::Connection) is a client.

pub(crate) mod os;
pub mod packet;
pub mod proxy;
pub(crate) mod proxy_core;
pub mod transport;

pub use packet::Packet;

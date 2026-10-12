//! The transports a host opens and the [`Packet`] they exchange.
//!
//! [`proto`](crate::proto) says what a packet means, and this module holds
//! what a host adds to it and moves it. Its types own their bytes and run
//! tasks. It does not decode samples or issue RPCs.
//!
//! - [`packet`] provides [`Packet`], one validated wire buffer read through
//!   the proto codecs. It also has the types a host adds,
//!   [`Payload`](packet::Payload), [`LinkStatus`](packet::LinkStatus), and
//!   [`ProxyRx`](packet::ProxyRx) for the proxy's control replies.
//! - [`rpc`] provides the codec of RPC arguments and replies.
//!
//! The transports are internal. Each port owns one serial, TCP, or UDP link,
//! which a task on the crate's I/O runtime drives.

pub(crate) mod os;
pub mod packet;
pub mod rpc;
pub(crate) mod transport;

pub use packet::Packet;

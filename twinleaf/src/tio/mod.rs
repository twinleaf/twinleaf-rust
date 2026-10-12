//! The transports a host opens and the [`Packet`] they exchange.
//!
//! [`proto`](crate::proto) says what a packet means, and this module moves
//! packets. Its types own their bytes and run tasks. It does not decode
//! samples or issue RPCs. [`device`](crate::device) does that, and an
//! application starts there. The [proxy](crate::proxy) shares each device
//! link among its clients.
//!
//! - [`packet`] provides [`Packet`], one validated wire buffer read through
//!   the proto codecs. It also has the types a host adds,
//!   [`Payload`](packet::Payload), [`LinkStatus`](packet::LinkStatus), and
//!   [`ProxyRx`](packet::ProxyRx) for the proxy's control replies.
//! - [`rpc`] provides the codec of RPC arguments and replies.
//!   [`device::rpc`](crate::device::rpc) re-exports it, and the proxy's
//!   control requests use it.
//!
//! The transports are internal. Each port owns one serial, TCP, or UDP link,
//! which a task on the crate's I/O runtime drives. A
//! [`Connection`](crate::Connection) opens one to the proxy, and the proxy
//! opens one to each device and one for each client.

pub(crate) mod os;
pub mod packet;
pub mod rpc;
pub(crate) mod transport;

pub use packet::Packet;

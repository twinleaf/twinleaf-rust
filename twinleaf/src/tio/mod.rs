//! The [`Packet`] a host exchanges with devices.
//!
//! [`proto`](crate::proto) says what a packet means, and this module holds
//! what a host adds to it. Its types own their bytes. It does not decode
//! samples or issue RPCs.
//!
//! - [`packet`] provides [`Packet`], one validated wire buffer read through
//!   the proto codecs. It also has the types a host adds,
//!   [`Payload`](packet::Payload), [`LinkStatus`](packet::LinkStatus), and
//!   [`ProxyRx`](packet::ProxyRx) for the proxy's control replies.
//! - [`rpc`] provides the codec of RPC arguments and replies.

pub mod packet;
pub mod rpc;

pub use packet::Packet;

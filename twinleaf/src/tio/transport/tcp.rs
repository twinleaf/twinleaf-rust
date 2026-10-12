//! The TCP transport.
//!
//! Connects the TCP streams that ports drive. TIO packets go on the stream
//! unchanged. The receiving end splits the stream into packets by the size
//! each header gives, as [`Codec`] does.

use super::codec::tcp::Codec;
use super::task::{ByteStream, Framing, Rules};
use super::{Packet, ReceiveResult, RecvError};
use crate::rt::{self, Supervisor};
use bytes::BytesMut;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::task::AbortOnDropHandle;

/// How long to wait for the handshake. It is one RTO plus slack, as long as
/// the proxy lets a TCP link stay silent.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// How long to wait for the handshake on a loopback address. A running proxy
/// completes it at once.
// A listening socket completes the handshake in the kernel before it
// accepts. A closed port can hang instead of refusing, as on WSL2 with
// mirrored networking, or take about 2 s to refuse, as on Windows.
pub(crate) const LOOPBACK_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// The rules of a TCP port's task. A TCP link has no heartbeat, holdoff or
/// stale data.
pub(super) const RULES: Rules = Rules::NONE;

/// How long the task of a client the proxy accepted waits for the client to
/// close.
pub(super) const CLIENT_HALF_CLOSE: Duration = Duration::from_millis(500);

/// The rules of the task of a client the proxy accepted. The link closes in
/// stages, so the client reads all the proxy sent before its socket closes.
pub(super) const CLIENT_RULES: Rules = Rules {
    half_close: Some(CLIENT_HALF_CLOSE),
    ..Rules::NONE
};

/// Connects to `address` and returns the stream. Waits at most
/// [`LOOPBACK_CONNECT_TIMEOUT`] for a loopback address and
/// [`CONNECT_TIMEOUT`] for any other.
///
/// # Errors
///
/// Returns an error if the connection fails.
pub(super) fn connect(address: &SocketAddr) -> io::Result<TcpStream> {
    // Connecting here, before the port exists, lets a dead address fail
    // the open rather than a port that looks connected.
    let stream = std::net::TcpStream::connect_timeout(address, connect_timeout(address))?;
    from_std(stream)
}

/// Connects to `address` as [`connect`] does, without blocking. The connect
/// runs as a task of the crate's runtime, which the stream belongs to.
/// Dropping the future aborts the task, which closes the socket.
///
/// # Errors
///
/// Returns an error if the connection fails, and
/// [`io::ErrorKind::TimedOut`] if it does not complete in time.
pub(super) async fn connect_async(address: SocketAddr) -> io::Result<TcpStream> {
    let timeout = connect_timeout(&address);
    let task = format!("tcp connect {address}");
    let connecting = Supervisor::new().spawn(task, async move {
        let connected = tokio::time::timeout(timeout, TcpStream::connect(address)).await;
        connected.unwrap_or_else(|_| {
            let message = format!("{address} did not answer within {timeout:?}");
            Err(io::Error::new(io::ErrorKind::TimedOut, message))
        })
    });
    let stream = match AbortOnDropHandle::new(connecting).await {
        Ok(Some(connected)) => connected?,
        // The supervisor logged the panic.
        Ok(None) => return Err(io::Error::other("the connect panicked")),
        Err(cancelled) => return Err(io::Error::other(cancelled)),
    };
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// Returns how long a connect to `address` waits for the handshake.
fn connect_timeout(address: &SocketAddr) -> Duration {
    match address.ip().is_loopback() {
        true => LOOPBACK_CONNECT_TIMEOUT,
        false => CONNECT_TIMEOUT,
    }
}

/// Returns the connected `stream` as a stream a port's task can drive, with
/// Nagle's algorithm off.
///
/// # Errors
///
/// Returns an error if the stream cannot be set up or registered with the
/// runtime.
pub(super) fn from_std(stream: std::net::TcpStream) -> io::Result<TcpStream> {
    // TIO packets are small and latency-sensitive (RPCs, heartbeats);
    // Nagle + delayed ACK would serialize a burst of requests.
    stream.set_nodelay(true)?;
    stream.set_nonblocking(true)?;
    let _runtime = rt::handle().enter();
    TcpStream::from_std(stream)
}

impl ByteStream for TcpStream {}

impl Framing for Codec {
    fn decode(&mut self, buf: &mut BytesMut) -> Option<ReceiveResult> {
        Codec::decode(self, buf)
            .map_err(RecvError::from)
            .transpose()
    }

    fn encode(&mut self, packet: &Packet, buf: &mut BytesMut) {
        Codec::encode(self, packet, buf);
    }
}

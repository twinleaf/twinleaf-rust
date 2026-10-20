//! The TCP transport.
//!
//! Connects the TCP streams that ports drive. TIO packets go on the stream
//! unchanged. The receiving end splits the stream into packets by the size
//! each header gives, as its [codec](super::codec::tcp::Codec) does.

use super::task::{ByteStream, Rules};
use crate::rt::{self, Supervisor};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::task::AbortOnDropHandle;

/// How long to wait for the handshake. It is one RTO plus slack, as long as
/// the proxy lets a TCP link stay silent.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

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
/// [`CONNECT_TIMEOUT`].
///
/// # Errors
///
/// Returns an error if the connection fails.
pub(super) fn connect(address: &SocketAddr) -> io::Result<TcpStream> {
    let address = *address;
    Supervisor::new().run("tcp connect", connect_stream(address))
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
    let task = format!("tcp connect {address}");
    let connecting = Supervisor::new().spawn(task, connect_stream(address));
    let stream = match AbortOnDropHandle::new(connecting).await {
        Ok(Some(connected)) => connected?,
        // The supervisor logged the panic.
        Ok(None) => return Err(io::Error::other("the connect panicked")),
        Err(cancelled) => return Err(io::Error::other(cancelled)),
    };
    Ok(stream)
}

/// Connects on the runtime that owns the link.
async fn connect_stream(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{address} did not answer within {CONNECT_TIMEOUT:?}"),
            )
        })??;
    stream.set_nodelay(true)?;
    Ok(stream)
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

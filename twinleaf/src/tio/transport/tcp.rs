//! The TCP transport.
//!
//! Connects the TCP streams that ports drive. TIO packets go on the stream
//! unchanged. The receiving end splits the stream into packets by the size
//! each header gives, as its [codec](super::codec::tcp::Codec) does.

use super::task::{ByteStream, Rules};
use crate::rt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

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

/// Connects to the first of `addresses` that answers, trying each in turn
/// for at most [`CONNECT_TIMEOUT`], and returns the stream. Must be awaited
/// on the crate's runtime, which the stream belongs to.
///
/// # Errors
///
/// Returns the error of the last address if none answers.
pub(super) async fn connect(addresses: &[SocketAddr]) -> io::Result<TcpStream> {
    let mut failed = io::Error::new(io::ErrorKind::NotFound, "no address to connect to");
    for &address in addresses {
        match connect_stream(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => failed = error,
        }
    }
    Err(failed)
}

/// Connects to `address` as [`TcpStream::connect`] does. A loopback address
/// that nothing listens on refuses the connection at once, on Windows too.
pub(crate) async fn connect_socket(address: SocketAddr) -> io::Result<TcpStream> {
    let socket = match address {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    if address.ip().is_loopback() {
        refuse_at_once(&socket);
    }
    socket.connect(address).await
}

/// Connects to `address` as [`std::net::TcpStream::connect_timeout`] does,
/// refusing at once like [`connect_socket`].
pub(crate) fn connect_blocking(
    address: &SocketAddr,
    timeout: Duration,
) -> io::Result<std::net::TcpStream> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(
        Domain::for_address(*address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    if address.ip().is_loopback() {
        refuse_at_once(&socket);
    }
    socket.connect_timeout(&(*address).into(), timeout)?;
    Ok(socket.into())
}

/// Makes a connect on `socket` give up after the first refusal. Windows
/// otherwise sends the handshake twice more, so a closed port takes about
/// 2 s to refuse.
#[cfg(windows)]
fn refuse_at_once(socket: &impl std::os::windows::io::AsRawSocket) {
    use windows_sys::Win32::Networking::WinSock::{
        WSAIoctl, SIO_TCP_INITIAL_RTO, TCP_INITIAL_RTO_PARAMETERS,
    };
    // mstcpip.h gives these as (USHORT)-1 and (UCHAR)-2. windows-sys has no
    // name for the first, and types the second as a u16.
    const UNSPECIFIED_RTT: u16 = u16::MAX;
    const NO_SYN_RETRANSMISSIONS: u8 = u8::MAX - 1;
    let parameters = TCP_INITIAL_RTO_PARAMETERS {
        Rtt: UNSPECIFIED_RTT,
        MaxSynRetransmissions: NO_SYN_RETRANSMISSIONS,
    };
    let mut returned = 0;
    // SAFETY: the socket is open, and the input is a TCP_INITIAL_RTO_PARAMETERS
    // of the size given. The call writes nothing but `returned`.
    let failed = unsafe {
        WSAIoctl(
            socket.as_raw_socket() as _,
            SIO_TCP_INITIAL_RTO,
            std::ptr::from_ref(&parameters).cast(),
            std::mem::size_of_val(&parameters) as u32,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
            None,
        )
    } != 0;
    if failed {
        log::debug!(
            "a loopback connect keeps retrying: {}",
            io::Error::last_os_error()
        );
    }
}

#[cfg(not(windows))]
fn refuse_at_once<T>(_socket: &T) {}

/// Connects to `address`, waiting at most [`CONNECT_TIMEOUT`].
async fn connect_stream(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, connect_socket(address))
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

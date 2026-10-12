//! Ports over an in-memory pipe, for tests.

use super::codec::tcp::Codec;
use super::task::{ByteStream, Rules};
use super::{Delivery, Port, RateInfo, TransportKind, DEFAULT_TX_CHANNEL_SIZE};
use crate::rt::Supervisor;
use crate::tio::packet::Packet;
use bytes::BytesMut;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::oneshot;

/// The size of a fake stream's pipe in each direction.
pub(super) const PIPE_SIZE: usize = 4096;

/// A stream over one end of an in-memory pipe. A read fails once the test
/// asks. Dropping the stream disconnects `_released`.
pub(super) struct FakeStream {
    pipe: DuplexStream,
    /// The receiver of the kind of error the next read fails with.
    fail: Option<oneshot::Receiver<io::ErrorKind>>,
    _released: mpsc::Sender<()>,
}

impl FakeStream {
    /// Returns a stream over `pipe`, the receiver that disconnects once the
    /// stream drops, and the sender that makes a read fail.
    pub(super) fn new(
        pipe: DuplexStream,
    ) -> (
        FakeStream,
        mpsc::Receiver<()>,
        oneshot::Sender<io::ErrorKind>,
    ) {
        let (released, gone) = mpsc::channel();
        let (fail, failing) = oneshot::channel();
        let stream = FakeStream {
            pipe,
            fail: Some(failing),
            _released: released,
        };
        (stream, gone, fail)
    }
}

impl AsyncRead for FakeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // What arrived before the failure is read first.
        if let Poll::Ready(read) = Pin::new(&mut self.pipe).poll_read(cx, buf) {
            return Poll::Ready(read);
        }
        if let Some(fail) = &mut self.fail {
            if let Poll::Ready(failed) = Pin::new(fail).poll(cx) {
                self.fail = None;
                if let Ok(kind) = failed {
                    return Poll::Ready(Err(kind.into()));
                }
            }
        }
        Poll::Pending
    }
}

impl AsyncWrite for FakeStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.pipe).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.pipe).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.pipe).poll_shutdown(cx)
    }
}

impl ByteStream for FakeStream {}

/// The test's end of a fake port's pipe. Dropping it closes the pipe, which
/// the port reads as its link closing.
pub(crate) struct FakeEnd {
    peer: DuplexStream,
    /// What the port wrote that is not yet a whole packet.
    written: BytesMut,
    fail: Option<oneshot::Sender<io::ErrorKind>>,
    /// Disconnects once the port's task has let go of its end.
    released: mpsc::Receiver<()>,
}

impl FakeEnd {
    /// Sends `packet` to the port.
    ///
    /// # Panics
    ///
    /// Panics if the port has let go of its end of the pipe.
    pub(crate) async fn send(&mut self, packet: &Packet) {
        let sent = self.peer.write_all(packet.as_bytes()).await;
        sent.expect("the port holds its end of the pipe");
    }

    /// Returns the next packet the port wrote, or `None` once the port has
    /// let go of its end of the pipe.
    ///
    /// # Panics
    ///
    /// Panics if what the port wrote does not decode.
    pub(crate) async fn recv(&mut self) -> Option<Packet> {
        loop {
            let decoded = Codec::default().decode(&mut self.written);
            if let Some(packet) = decoded.expect("the port writes packets") {
                return Some(packet);
            }
            match self.peer.read_buf(&mut self.written).await {
                Ok(0) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }

    /// Makes the port's next read fail with an error of `kind`. Later calls
    /// do nothing.
    pub(crate) fn fail(&mut self, kind: io::ErrorKind) {
        if let Some(fail) = self.fail.take() {
            let _ = fail.send(kind);
        }
    }

    /// Returns `true` once the port's task has let go of its end of the
    /// pipe.
    pub(crate) fn released(&self) -> bool {
        matches!(
            self.released.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        )
    }
}

impl Port {
    /// Returns a port of `kind` with `rates` over an in-memory pipe, and the
    /// test's end of the pipe. The port's task runs on the current runtime,
    /// with no heartbeat or holdoff, and frames packets as TCP does. It
    /// delivers what it receives to `rx`.
    ///
    /// # Panics
    ///
    /// Panics if it is called outside a runtime.
    pub(crate) fn fake(
        kind: TransportKind,
        rates: Option<RateInfo>,
        rx: Delivery,
    ) -> (Port, FakeEnd) {
        let (pipe, peer) = tokio::io::duplex(PIPE_SIZE);
        let (stream, released, fail) = FakeStream::new(pipe);
        let (port, task) = Port::with_task(kind, rates, Rules::NONE, rx, DEFAULT_TX_CHANNEL_SIZE);
        let run = task.run_stream(stream, Codec::default());
        let port = port.start(&Supervisor::current(), "a fake port", run);
        let end = FakeEnd {
            peer,
            written: BytesMut::new(),
            fail: Some(fail),
            released,
        };
        (port, end)
    }
}

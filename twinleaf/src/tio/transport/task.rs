//! The task that drives a port's link.
//!
//! Each [`Port`](super::Port) has one task on the crate's runtime, which owns
//! the link and its buffers. The task hands what arrives to the port's
//! receiver. It writes what the port queues one packet at a time, sends
//! heartbeats, and keeps the startup holdoff. It ends when the link closes,
//! when nothing receives what it reads, or once the port is dropped and the
//! packets it queued are written. The link closes as the task ends. A link
//! whose [`Rules`] close it in stages first waits a moment for its peer to
//! close.
//!
//! [`run_stream`](Task::run_stream) drives a TCP or serial byte stream, and
//! [`run_datagram`](Task::run_datagram) drives a UDP socket.

use super::codec::udp as datagram;
use super::{
    udp, Delivery, PacketOrControl, ReceiveResult, RecvError, TransportEvent, DRAIN_TIMEOUT,
};
use crate::proto::DeviceRoute;
use crate::rt::{self, sleep_until_some};
use crate::tio::packet::Packet;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tokio_util::udp::UdpFramed;

/// What a port's task does beside moving packets, which depends on its link.
pub(super) struct Rules {
    /// The longest the link may go without sending, if it has a limit. A
    /// heartbeat goes out when it passes.
    pub heartbeat: Option<Duration>,
    /// How long after the port opens it discards what it receives, but for
    /// text, and writes nothing but heartbeats. The first bytes to arrive in
    /// that time are discarded whole.
    pub holdoff: Option<Duration>,
    /// How long received bytes wait for the rest of their frame. Bytes held
    /// longer are discarded when more arrive.
    #[cfg_attr(not(feature = "serial"), allow(dead_code))]
    pub stale_after: Option<Duration>,
    /// How long the task waits for the peer to close, if the link closes in
    /// stages. Once the port is dropped and its queue is written, the task
    /// shuts down its sending side. The peer then reads all the task wrote,
    /// and then the end of the stream. The task discards what arrives while
    /// it waits.
    pub half_close: Option<Duration>,
}

impl Rules {
    /// The rules of a link with no heartbeat, holdoff, stale data or close in
    /// stages.
    pub const NONE: Rules = Rules {
        heartbeat: None,
        holdoff: None,
        stale_after: None,
        half_close: None,
    };
}

/// Returns the error of a link whose rate cannot change.
fn rate_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "rate changes not supported on this port",
    )
}

/// A byte stream that a port's task reads and writes, TCP or serial.
pub(super) trait ByteStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    /// Sets the data rate on the host side only. Changing the device's rate
    /// takes RPCs of its own.
    ///
    /// # Errors
    ///
    /// Returns an [`io::ErrorKind::Unsupported`] error if the stream cannot
    /// change its rate, and the error of a rate change that fails.
    fn set_rate(&mut self, _rate: u32) -> io::Result<()> {
        Err(rate_unsupported())
    }

    /// Returns what a failed read of the stream reports.
    fn read_error(error: io::Error) -> RecvError {
        RecvError::IO(error)
    }
}

/// The state of a port's task beside its link.
pub(super) struct Task {
    /// Where the task delivers what the link receives.
    rx: Delivery,
    /// The packets and rate changes the port queues.
    queue: mpsc::Receiver<PacketOrControl>,
    /// The longest the link may go without sending, if it has a limit.
    heartbeat: Option<Duration>,
    /// When the next heartbeat is due, if the link has a heartbeat.
    next_heartbeat: Instant,
    /// When the startup holdoff ends, while it lasts.
    holdoff: Option<Instant>,
    /// How long the task waits for the peer to close, if the link closes in
    /// stages.
    half_close: Option<Duration>,
    /// The hold that keeps the system awake while the link is open.
    _activity: rt::Activity,
}

impl Task {
    /// Returns a task that delivers what its link receives to `rx` and
    /// writes what arrives on `queue`, by `rules`.
    pub(super) fn new(rx: Delivery, queue: mpsc::Receiver<PacketOrControl>, rules: Rules) -> Task {
        let now = Instant::now();
        // Heartbeats go out during the holdoff, which helps a device switch
        // to binary mode before it ends. The first goes out at once.
        let next_heartbeat = match rules.holdoff {
            Some(_) => now,
            None => now + rules.heartbeat.unwrap_or_default(),
        };
        Task {
            rx,
            queue,
            heartbeat: rules.heartbeat,
            next_heartbeat,
            holdoff: rules.holdoff.map(|holdoff| now + holdoff),
            half_close: rules.half_close,
            _activity: rt::Activity::hold(),
        }
    }

    /// Hands `result` to the port's receiver, and drops it if the receiver's
    /// channel is full. During the holdoff it drops packets and rejected frames,
    /// but passes text and terminal errors. Returns `false` after a terminal
    /// error or once nothing receives.
    fn deliver(&self, result: ReceiveResult) -> bool {
        let terminal = result.is_err();
        if self.holdoff.is_some() && !terminal && !matches!(result, Ok(TransportEvent::Text(_))) {
            return true;
        }
        let receiving = !matches!(
            self.rx.try_send(result),
            Err(flume::TrySendError::Disconnected(_))
        );
        receiving && !terminal
    }

    /// Returns `true` if a heartbeat is due at `now`.
    fn heartbeat_due(&self, now: Instant) -> bool {
        self.heartbeat.is_some() && self.next_heartbeat <= now
    }

    /// Puts off the next heartbeat after a write that finished at `now`.
    fn wrote(&mut self, now: Instant) {
        if let Some(interval) = self.heartbeat {
            self.next_heartbeat = now + interval;
        }
    }

    /// Returns when the task must wake to end the holdoff or send a
    /// heartbeat. No heartbeat is due while `writing`.
    fn next_wake(&self, writing: bool) -> Option<Instant> {
        let heartbeat = self
            .heartbeat
            .filter(|_| !writing)
            .map(|_| self.next_heartbeat);
        heartbeat.into_iter().chain(self.holdoff).min()
    }

    /// Ends the holdoff once its time has come.
    fn wake(&mut self) {
        let now = Instant::now();
        self.holdoff = self.holdoff.filter(|&end| end > now);
    }

    /// Runs the link through Tokio's framed stream and sink. A queued packet
    /// is fed once, then flushed while receiving remains responsive.
    pub(super) async fn run_stream<S, C>(mut self, stream: S, codec: C)
    where
        S: ByteStream,
        C: Decoder<Item = TransportEvent, Error = RecvError>
            + Encoder<Packet, Error = RecvError>
            + Send
            + Unpin
            + 'static,
    {
        let (mut sink, mut source) = Framed::new(stream, codec).split();
        let mut writing = false;
        let mut write_by = Instant::now();
        loop {
            let now = Instant::now();
            if !writing && self.heartbeat_due(now) {
                if let Err(error) = sink.feed(Packet::heartbeat(DeviceRoute::root())).await {
                    self.deliver(Err(error));
                    break;
                }
                writing = true;
                write_by = now + DRAIN_TIMEOUT;
            }
            let wake = self.next_wake(writing);
            tokio::select! {
                received = source.next() => {
                    let result = match received {
                        Some(Err(RecvError::IO(error))) => Err(S::read_error(error)),
                        Some(result) => result,
                        None => break,
                    };
                    if !self.deliver(result) {
                        break;
                    }
                }
                flushed = timeout_at(write_by, sink.flush()), if writing => {
                    match flushed {
                        Ok(Ok(())) => {
                            writing = false;
                            self.wrote(Instant::now());
                        }
                        Ok(Err(error)) => {
                            self.deliver(Err(error));
                            break;
                        }
                        Err(_) => {
                            self.deliver(Err(io::Error::new(io::ErrorKind::TimedOut, "port write deadline expired").into()));
                            break;
                        }
                    }
                }
                message = self.queue.recv(), if !writing && self.holdoff.is_none() => {
                    match message {
                        Some(PacketOrControl::Pkt(packet)) => {
                            // The preceding packet was flushed, so feed has room
                            // immediately. Only flush waits for the peer to read.
                            if let Err(error) = sink.feed(packet).await {
                                self.deliver(Err(error));
                                break;
                            }
                            writing = true;
                            write_by = Instant::now() + DRAIN_TIMEOUT;
                        }
                        Some(PacketOrControl::SetRate(rate, result)) => {
                            let mut framed = sink.reunite(source)
                                .unwrap_or_else(|_| unreachable!("matching framed halves"));
                            let _ = result.send(framed.get_mut().set_rate(rate));
                            (sink, source) = framed.split();
                        }
                        None => {
                            if let Some(within) = self.half_close {
                                close_in_stages(&mut sink, &mut source, within).await;
                            }
                            break;
                        }
                    }
                }
                () = sleep_until_some(wake) => self.wake(),
            }
        }
    }

    /// Runs a UDP link through Tokio's datagram stream and sink.
    /// Advisory ICMP errors leave the device watchdog to judge the link.
    pub(super) async fn run_datagram(mut self, socket: UdpSocket, peer: std::net::SocketAddr) {
        let (mut sink, mut source) = UdpFramed::new(socket, datagram::Codec::default()).split();
        let mut writing = false;
        let mut write_by = Instant::now();
        loop {
            let now = Instant::now();
            if !writing && self.heartbeat_due(now) {
                if let Err(error) = sink
                    .feed((Packet::heartbeat(DeviceRoute::root()), peer))
                    .await
                {
                    self.deliver(Err(error));
                    break;
                }
                writing = true;
                write_by = now + DRAIN_TIMEOUT;
            }
            let wake = self.next_wake(writing);
            tokio::select! {
                received = source.next() => {
                    let result = match received {
                        Some(Ok((_, from))) if from != peer => continue,
                        Some(Ok((event, _))) => Ok(event),
                        Some(Err(RecvError::IO(error))) if udp::is_advisory_network_error(&error) => continue,
                        Some(Err(error)) => Err(error),
                        None => break,
                    };
                    if !self.deliver(result) { break; }
                }
                flushed = timeout_at(write_by, sink.flush()), if writing => {
                    match flushed {
                        Ok(Ok(())) => {}
                        Ok(Err(RecvError::IO(error))) if udp::is_advisory_network_error(&error) => {
                            // UdpFramed retains an unsent datagram after an error.
                            // Drop that datagram while keeping the socket.
                            let socket = sink.reunite(source)
                                .unwrap_or_else(|_| unreachable!("matching datagram halves"))
                                .into_inner();
                            (sink, source) = UdpFramed::new(socket, datagram::Codec::default()).split();
                        }
                        Ok(Err(error)) => { self.deliver(Err(error)); break; }
                        Err(_) => {
                            self.deliver(Err(io::Error::new(io::ErrorKind::TimedOut, "port write deadline expired").into()));
                            break;
                        }
                    }
                    writing = false;
                    self.wrote(Instant::now());
                }
                message = self.queue.recv(), if !writing && self.holdoff.is_none() => {
                    match message {
                        Some(PacketOrControl::Pkt(packet)) => {
                            if let Err(error) = sink.feed((packet, peer)).await {
                                self.deliver(Err(error)); break;
                            }
                            writing = true;
                            write_by = Instant::now() + DRAIN_TIMEOUT;
                        }
                        Some(PacketOrControl::SetRate(_, result)) => {
                            let _ = result.send(Err(rate_unsupported()));
                        }
                        None => break,
                    }
                }
                () = sleep_until_some(wake) => self.wake(),
            }
        }
    }
}

/// Finishes the framed sink, then consumes the peer's stream until EOF or
/// the close deadline. Tokio owns the flushing and socket shutdown.
async fn close_in_stages<T>(
    sink: &mut SplitSink<T, Packet>,
    source: &mut SplitStream<T>,
    within: Duration,
) where
    T: Stream<Item = ReceiveResult> + Sink<Packet, Error = RecvError> + Unpin,
{
    let closing = async {
        if sink.close().await.is_ok() {
            source.for_each(|_| std::future::ready(())).await;
        }
    };
    let _ = timeout_at(Instant::now() + within, closing).await;
}

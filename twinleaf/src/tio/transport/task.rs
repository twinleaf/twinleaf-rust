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
use super::{udp, Delivery, PacketOrControl, ReceiveResult, RecvError, DRAIN_TIMEOUT};
use crate::proto::DeviceRoute;
use crate::rt::{self, sleep_until_some};
use crate::tio::packet::Packet;
use bytes::{Buf, BytesMut};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};

/// The most bytes a stream port reads at once.
const READ_SIZE: usize = 4096;

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

/// The framing of packets on a [`ByteStream`].
pub(super) trait Framing: Default + Send + 'static {
    /// Decodes the next packet or error at the start of `buf`, and removes
    /// the bytes it used. Returns `None` once `buf` holds nothing more to
    /// decode.
    fn decode(&mut self, buf: &mut BytesMut) -> Option<ReceiveResult>;

    /// Appends the frame of `packet` to `buf`.
    fn encode(&mut self, packet: &Packet, buf: &mut BytesMut);
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
    /// How long received bytes wait for the rest of their frame.
    stale_after: Option<Duration>,
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
            stale_after: rules.stale_after,
            half_close: rules.half_close,
            _activity: rt::Activity::hold(),
        }
    }

    /// Hands `result` to the port's receiver, and drops it if the receiver's
    /// channel is full. During the holdoff it drops everything but text and
    /// a disconnect. Returns `false` once the link has disconnected or
    /// nothing receives.
    fn deliver(&self, result: ReceiveResult) -> bool {
        let passes = matches!(result, Err(RecvError::Text(_) | RecvError::Disconnected));
        if self.holdoff.is_some() && !passes {
            return true;
        }
        if let Err(RecvError::Disconnected) = result {
            return false;
        }
        !matches!(
            self.rx.try_send(result),
            Err(flume::TrySendError::Disconnected(_))
        )
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

    /// Runs the port over `stream`, with packets framed by `framing`, until
    /// the task ends.
    pub(super) async fn run_stream<S: ByteStream, F: Framing>(mut self, stream: S, mut framing: F) {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut received = BytesMut::new();
        let mut sending = BytesMut::new();
        // When the packet in `sending` must be written by.
        let mut write_by = Instant::now();
        let mut last_read = Instant::now();
        let mut first_read = true;
        'run: loop {
            let now = Instant::now();
            if sending.is_empty() && self.heartbeat_due(now) {
                framing.encode(&Packet::heartbeat(DeviceRoute::root()), &mut sending);
                write_by = now + DRAIN_TIMEOUT;
            }
            let wake = self.next_wake(!sending.is_empty());
            let held = received.len();
            received.reserve(READ_SIZE);
            tokio::select! {
                read = reader.read_buf(&mut received) => {
                    let now = Instant::now();
                    match read {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(error) => {
                            if !self.deliver(Err(S::read_error(error))) {
                                break;
                            }
                            continue;
                        }
                    }
                    if self.holdoff.is_some() {
                        self.next_heartbeat = now;
                    }
                    if std::mem::take(&mut first_read) && self.holdoff.is_some() {
                        // The first bytes are likely stale, or corrupted by
                        // the driver as the port opened. Decoding them could
                        // stall the decoder until a large frame fails.
                        received.clear();
                        framing = F::default();
                        continue;
                    }
                    if self.stale_after.is_some_and(|stale| now.duration_since(last_read) > stale) {
                        // What was held is left from before a pause, as when
                        // a board is reprogrammed in the middle of a packet.
                        received.advance(held);
                        framing = F::default();
                    }
                    last_read = now;
                    while let Some(result) = framing.decode(&mut received) {
                        if !self.deliver(result) {
                            break 'run;
                        }
                    }
                }
                written = timeout_at(write_by, writer.write_buf(&mut sending)), if !sending.is_empty() => {
                    match written {
                        Ok(Ok(size)) if size > 0 => {
                            if sending.is_empty() {
                                self.wrote(Instant::now());
                            }
                        }
                        // A failed write, a stream that takes nothing, or a
                        // packet not written in time gives up the link.
                        _ => break,
                    }
                }
                message = self.queue.recv(), if sending.is_empty() && self.holdoff.is_none() => {
                    match message {
                        Some(PacketOrControl::Pkt(packet)) => {
                            framing.encode(&packet, &mut sending);
                            write_by = Instant::now() + DRAIN_TIMEOUT;
                        }
                        Some(PacketOrControl::SetRate(rate, result)) => {
                            let mut stream = reader.unsplit(writer);
                            let _ = result.send(stream.set_rate(rate));
                            (reader, writer) = tokio::io::split(stream);
                        }
                        None => {
                            if let Some(within) = self.half_close {
                                close_in_stages(&mut reader, &mut writer, within).await;
                            }
                            break;
                        }
                    }
                }
                () = sleep_until_some(wake) => self.wake(),
            }
        }
    }

    /// Runs the port over the connected UDP `socket` until the task ends.
    ///
    /// # Panics
    ///
    /// Panics if the socket sends only part of a datagram.
    pub(super) async fn run_datagram(mut self, socket: UdpSocket) {
        let mut received = [0; udp::RX_SIZE];
        // The datagram being sent, empty once it has gone.
        let mut sending = BytesMut::new();
        let mut write_by = Instant::now();
        loop {
            let now = Instant::now();
            if sending.is_empty() && self.heartbeat_due(now) {
                let heartbeat = Packet::heartbeat(DeviceRoute::root());
                sending.extend_from_slice(datagram::encode(&heartbeat));
                write_by = now + DRAIN_TIMEOUT;
            }
            let wake = self.next_wake(!sending.is_empty());
            tokio::select! {
                read = socket.recv(&mut received) => {
                    let result = match read {
                        Ok(size) => datagram::decode(&received[..size]).map_err(RecvError::from),
                        Err(error) if udp::is_advisory_network_error(&error) => continue,
                        Err(error) => Err(RecvError::IO(error)),
                    };
                    if !self.deliver(result) {
                        break;
                    }
                }
                sent = timeout_at(write_by, socket.send(&sending)), if !sending.is_empty() => {
                    match sent {
                        Ok(Ok(size)) => {
                            assert_eq!(size, sending.len(), "Unexpected UDP short write");
                        }
                        Ok(Err(error)) if udp::is_advisory_network_error(&error) => {}
                        _ => break,
                    }
                    sending.clear();
                    self.wrote(Instant::now());
                }
                message = self.queue.recv(), if sending.is_empty() && self.holdoff.is_none() => {
                    match message {
                        Some(PacketOrControl::Pkt(packet)) => {
                            sending.extend_from_slice(datagram::encode(&packet));
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

/// Shuts down the sending side of the link that `reader` and `writer` split,
/// then reads and discards what arrives until the peer closes its side, for
/// at most `within`.
// A close with input still unread resets the link, and the reset can
// overtake what was written last. A peer that has closed has read all of
// it, and sends nothing more.
async fn close_in_stages<S: ByteStream>(
    reader: &mut ReadHalf<S>,
    writer: &mut WriteHalf<S>,
    within: Duration,
) {
    let mut discarded = BytesMut::with_capacity(READ_SIZE);
    let closing = async {
        if writer.shutdown().await.is_err() {
            return;
        }
        loop {
            discarded.clear();
            discarded.reserve(READ_SIZE);
            match reader.read_buf(&mut discarded).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    };
    let _ = timeout_at(Instant::now() + within, closing).await;
}

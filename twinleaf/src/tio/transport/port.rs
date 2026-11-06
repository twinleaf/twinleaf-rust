//! The [`Port`], the host's end of one link, and how each kind of link opens.

#[cfg(feature = "serial")]
use super::serial;
use super::task::{Rules, Task};
use super::{
    codec, ip_address, preferred, resolve, tcp, udp, AddrFamilyRestrict, Delivery, IpLink,
    KeepAlive, PacketOrControl, RateInfo, SendError, TransportKind, DEFAULT_TX_CHANNEL_SIZE,
};
use crate::chan::{oneshot, JoinHandle};
use crate::rt::Supervisor;
use crate::tio::packet::Packet;
use std::fmt::Display;
use std::future::Future;
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;

/// One link and the task that drives it. Packets arrive on a channel and
/// leave through [`try_send`](Self::try_send). Dropping the port closes the
/// link once the task has written what was queued, and
/// [`close`](Self::close) also waits for the task to end.
pub struct Port {
    /// The queue of packets and rate changes to the task.
    tx: mpsc::Sender<PacketOrControl>,
    /// The task, whose link is closed once it has ended.
    task: JoinHandle<Option<()>>,
    rates: Option<RateInfo>,
    kind: TransportKind,
}

/// A port whose task has not started.
pub(super) struct Unstarted {
    tx: mpsc::Sender<PacketOrControl>,
    rates: Option<RateInfo>,
    kind: TransportKind,
}

impl Unstarted {
    /// Spawns `run`, the port's task, on `tasks` as the task named `name`,
    /// and returns the port.
    pub(super) fn start(
        self,
        tasks: &Supervisor,
        name: impl Display,
        run: impl Future<Output = ()> + Send + 'static,
    ) -> Port {
        Port {
            tx: self.tx,
            task: tasks.spawn(name, run),
            rates: self.rates,
            kind: self.kind,
        }
    }
}

impl Port {
    /// Returns a port of the given `kind` and `rates` that has not started,
    /// and the state of its task, which drives the link by `rules`. The task
    /// delivers what it receives to `rx`. The port queues up to `tx_size`
    /// packets to it.
    pub(super) fn with_task(
        kind: TransportKind,
        rates: Option<RateInfo>,
        rules: Rules,
        rx: Delivery,
        tx_size: usize,
    ) -> (Unstarted, Task) {
        let (tx, queue) = mpsc::channel(tx_size.max(1));
        let port = Unstarted { tx, rates, kind };
        (port, Task::new(rx, queue, rules))
    }

    #[cfg(feature = "serial")]
    fn open_serial(serial_config: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        let (stream, rates) = serial::open(serial_config, tasks)?;
        let (port, task) = Port::with_task(
            TransportKind::Serial,
            Some(rates),
            serial::RULES,
            rx,
            DEFAULT_TX_CHANNEL_SIZE,
        );
        let framing =
            codec::serial::Codec::with_timing(serial::RULES.holdoff, serial::RULES.stale_after);
        let run = task.run_stream(stream, framing);
        Ok(port.start(tasks, format_args!("twinleaf port {serial_config}"), run))
    }

    /// Opens a [`Port`] over `link` to `address`, with its task on `tasks`.
    /// Tries each address `address` resolves to in turn.
    async fn open_ip(
        link: IpLink,
        address: &str,
        family: AddrFamilyRestrict,
        rx: Delivery,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        let found = resolve(address, family, tasks).await?;
        match link {
            IpLink::Tcp => {
                let stream = tcp::connect(&found).await?;
                Ok(Port::start_tcp(
                    stream,
                    tcp::RULES,
                    rx,
                    DEFAULT_TX_CHANNEL_SIZE,
                    tasks,
                ))
            }
            IpLink::Udp => {
                // An unspecified address, which `preferred` skips, is still
                // sent to when it is all the name resolves to.
                let peer = preferred(&found, &family).unwrap_or(found[0]);
                let socket = udp::bind(&peer).await?;
                let (port, task) = Port::with_task(
                    TransportKind::Udp,
                    None,
                    udp::RULES,
                    rx,
                    DEFAULT_TX_CHANNEL_SIZE,
                );
                let run = task.run_datagram(socket, peer);
                Ok(port.start(tasks, format_args!("twinleaf port udp://{peer}"), run))
            }
        }
    }

    /// Starts the task of a port over the TCP `stream` by `rules` on `tasks`.
    /// The task delivers what it receives to `rx`, and the port queues up to
    /// `tx_size` packets to send.
    fn start_tcp(
        stream: crate::net::TcpStream,
        rules: Rules,
        rx: Delivery,
        tx_size: usize,
        tasks: &Supervisor,
    ) -> Port {
        let peer = stream
            .peer_addr()
            .map_or(String::new(), |peer| peer.to_string());
        let (port, task) = Port::with_task(TransportKind::Tcp, None, rules, rx, tx_size);
        let run = task.run_stream(stream, codec::tcp::Codec::default());
        port.start(tasks, format_args!("twinleaf port tcp://{peer}"), run)
    }

    /// Opens a [`Port`] for the device or proxy at `url`. The port delivers
    /// what it receives to `rx` and drops what does not fit. It closes once
    /// nothing receives from `rx`. Blocks while it resolves the address and
    /// opens the link.
    ///
    /// A valid `url` has one of these formats.
    ///
    /// - `serial://port[:target_bps[:default_bps]]`, which requires the
    ///   `serial` feature. `target_bps` and `default_bps` default to 115200.
    ///   `serial://` may be omitted for a port that starts with `COM` on
    ///   Windows or `/dev/` on Unix.
    /// - `tcp://address[:port]`. The schemes `tcp4` and `tcp6` force IPv4 or
    ///   IPv6.
    /// - `udp://address[:port]`. The schemes `udp4` and `udp6` force IPv4 or
    ///   IPv6.
    ///
    /// Its task runs on `tasks`.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) fn new_on(url: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        // Special case: serial ports can be given directly
        #[cfg(all(feature = "serial", unix))]
        if url.starts_with("/dev/") {
            return Self::open_serial(url, rx, tasks);
        }
        #[cfg(all(feature = "serial", target_os = "windows"))]
        if url.starts_with("COM") {
            return Self::open_serial(url, rx, tasks);
        }

        if Port::opens_as_future(url) {
            let (url, on) = (url.to_string(), tasks.clone());
            return tasks.run("twinleaf port open", async move {
                Port::open(&url, rx, &on).await
            });
        }
        match url.split_once("://") {
            #[cfg(feature = "serial")]
            Some(("serial", port)) => Self::open_serial(port, rx, tasks),
            #[cfg(not(feature = "serial"))]
            Some(("serial", _)) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "serial support is disabled",
            )),
            Some(_) | None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid TIO transport locator: {url}"),
            )),
        }
    }

    /// Opens a [`Port`] for `url` as [`new_on`](Self::new_on) does, without
    /// blocking a worker of the runtime. A TCP or UDP link resolves its
    /// address and opens as a future, and dropping it at any await leaves
    /// nothing open. Its task runs on `tasks`. Any other link opens on a
    /// blocking thread of `tasks`.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) async fn open(url: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        let Some((link, address, family)) = ip_address(url) else {
            let (url, on) = (url.to_string(), tasks.clone());
            let opening = tasks.spawn_blocking(move || Port::new_on(&url, rx, &on));
            return opening.await.map_err(io::Error::other)?;
        };
        Port::open_ip(link, address, family, rx, tasks).await
    }

    /// Returns `true` if dropping an [`open`](Self::open) of `url` in flight
    /// leaves nothing open, as for a TCP or UDP link.
    pub(crate) fn opens_as_future(url: &str) -> bool {
        ip_address(url).is_some()
    }

    /// Closes the port. Waits until its task has written what was queued and
    /// closed the link, for at most `within`. Then ends the task, which
    /// closes the link at once. Returns `true` if the task ended in time.
    pub(crate) async fn close(self, within: Duration) -> bool {
        let Port { tx, mut task, .. } = self;
        drop(tx);
        if tokio::time::timeout(within, &mut task).await.is_ok() {
            return true;
        }
        // An aborted task has dropped its link by the time its handle
        // resolves.
        task.abort();
        let _ = task.await;
        false
    }

    /// Creates a [`Port`] over the connected TCP `stream`, such as the one a
    /// control call opens to the proxy, with Nagle's algorithm off. Its task
    /// runs on `tasks`. It delivers what it receives to `rx` as
    /// [`new_on`](Self::new_on) does, and queues up to `tx_size` packets to
    /// send.
    ///
    /// # Errors
    ///
    /// Returns an error if Nagle's algorithm cannot be turned off.
    pub(crate) fn over_tcp(
        stream: crate::net::TcpStream,
        rx: Delivery,
        tx_size: usize,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        // TIO packets are small and latency sensitive, and Nagle's algorithm
        // with delayed acknowledgement would serialize a burst of requests.
        stream.set_nodelay(true)?;
        Ok(Port::start_tcp(stream, tcp::RULES, rx, tx_size, tasks))
    }

    /// Creates a [`Port`] over the connected TCP `stream`, a socket of no
    /// runtime yet, as [`over_tcp`](Self::over_tcp) does on the crate's
    /// runtime.
    #[cfg(all(test, not(turmoil)))]
    pub(crate) fn from_tcp_stream(
        stream: std::net::TcpStream,
        rx: Delivery,
        tx_size: usize,
    ) -> io::Result<Port> {
        let tasks = Supervisor::new();
        stream.set_nonblocking(true)?;
        let stream = {
            let _runtime = tasks.enter();
            crate::net::TcpStream::from_std(stream)?
        };
        Port::over_tcp(stream, rx, tx_size, &tasks)
    }

    /// Creates a [`Port`] over `stream`, a connected TCP stream of the
    /// runtime of `tasks`, such as a client the proxy accepted. Its task runs
    /// on `tasks`. It delivers what it receives to `rx` as
    /// [`new_on`](Self::new_on) does, and queues up to `tx_size` packets to
    /// send. Once the port is dropped and its queue is written, the link
    /// closes in stages. The task
    /// shuts down its sending side, and waits up to 500 ms for the client to
    /// close first.
    ///
    /// # Errors
    ///
    /// Returns an error if Nagle's algorithm cannot be turned off.
    pub(crate) fn from_tcp(
        stream: crate::net::TcpStream,
        rx: Delivery,
        tx_size: usize,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        stream.set_nodelay(true)?;
        Ok(Port::start_tcp(
            stream,
            tcp::CLIENT_RULES,
            rx,
            tx_size,
            tasks,
        ))
    }

    /// Sends `packet` to the port, and waits while the port's queue is full.
    /// The wait holds no borrow of the port.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Disconnected`] if the port's task has stopped.
    pub(crate) fn send_async(&self, packet: Packet) -> impl Future<Output = Result<(), SendError>> {
        let tx = self.tx.clone();
        async move {
            let sent = tx.send(PacketOrControl::Pkt(packet)).await;
            sent.map_err(|_| SendError::Disconnected)
        }
    }

    /// Sends `packet` to the port without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Full`] if the queue is full, and
    /// [`SendError::Disconnected`] if the port's task has stopped.
    pub fn try_send(&self, packet: Packet) -> Result<(), SendError> {
        self.tx
            .try_send(PacketOrControl::Pkt(packet))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SendError::Full,
                mpsc::error::TrySendError::Closed(_) => SendError::Disconnected,
            })
    }

    /// Returns the kind of link this port talks over.
    pub(crate) fn kind(&self) -> TransportKind {
        self.kind
    }

    /// Returns the link's rate information, or `None` if its rate cannot
    /// change.
    pub(crate) fn rate_info(&self) -> Option<RateInfo> {
        self.rates.clone()
    }

    /// Has the link send heartbeats and give up a quiet peer from now on, as
    /// `keep_alive` says. A port whose task has stopped has nothing to keep
    /// alive, and its receiver hears that it ended.
    pub(crate) async fn keep_alive(&self, keep_alive: KeepAlive) {
        let _ = self.tx.send(PacketOrControl::KeepAlive(keep_alive)).await;
    }

    /// Sets the link's data rate on the host side, once the packets queued
    /// before are written, and waits until the task has applied it.
    ///
    /// # Errors
    ///
    /// Returns an [`io::ErrorKind::BrokenPipe`] error if the port has
    /// closed, and an error if the link's rate cannot change.
    pub(crate) async fn set_rate(&self, rate: u32) -> io::Result<()> {
        let closed = || io::Error::new(io::ErrorKind::BrokenPipe, "the port closed");
        let (result, applied) = oneshot::channel();
        let queued = self.tx.send(PacketOrControl::SetRate(rate, result)).await;
        queued.map_err(|_| closed())?;
        applied.await.map_err(|_| closed())?
    }
}

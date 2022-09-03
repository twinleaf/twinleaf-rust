//! Abstract port
//!
//! The `Port` object provides a few things:
//! - Abstracting across the specific `RawPort`s. This abstraction
//!   is powerful enough that it should never be needed to use
//!   the low level `RawPort` object.
//! - Connecting the ports to external code in a flexible way.
//!   This is achieved via a owned-callback interface, and it
//!   importantly allows to bridge the `mio` world of the low level
//!   ports with crossbeam channels.
//! - Automating some basic port operations. A `Port` provides
//!   polling, send queues, as-needed port draining, and the
//!   sending of heartbeats as needed to satisfy periodic packet
//!   receiving requirements.
//!
//! Note: `Port` sets up a dedicated thread to perform the above.

// TODO: finish rustdoc comments

mod iobuf;
mod serial;
mod tcp;
mod udp;

use super::proto::{self, Packet};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

/// Possible errors when receiving from a `Port`
#[derive(Debug)]
pub enum RecvError {
    /// No packets available at this time.
    NotReady,
    /// This port got disconnected.
    Disconnected,
    /// Error in the data.
    Protocol(proto::Error),
    /// Low level IO error.
    IO(io::Error),
}

/// Possible errors when sending to a `Port`
#[derive(Debug)]
pub enum SendError {
    /// This is used internally and should never happen for a `Port`.
    /// Low level RawPort
    /// This should never happen for a `Port`, only for the internal low level `RawPort`.
    MustDrain,
    /// The port outgoing queue is full
    Full,
    Disconnected,
    IO(io::Error),
}

/// Possible errors when setting a custom data rate
#[derive(Debug)]
pub enum RateError {
    Unsupported,
    InvalidRate,
    Failed,
}

/// Custom data rate info associated with the port
#[derive(Clone)]
pub struct RateInfo {
    /// Default/fallback rate
    pub default_bps: u32,
    /// Target rate.
    /// If communication fail at any point, the port should fall back to `default_bps`.
    pub target_bps: u32,
}

/// Generic interface for the low level part of a port.
pub trait RawPort {
    /// Returns a packet without blocking, or RecvError::NotReady if one is not available.
    /// For all the other error values, the port should be torn down, and possibly recreated.
    fn recv(&mut self) -> Result<Packet, RecvError>;

    /// Attempts to send a packet. If it doesn't return Ok:
    /// - if it returned MustDrain, the packet was sent partially, and must be drained manually via drain()
    /// - if it returned Full, the last packet written was MustDrain and it hasn't been drained yet
    /// - for all other errors, the appropriate action is to tear down this port and recreate.
    fn send(&mut self, pkt: &Packet) -> Result<(), SendError>;

    /// Drain partially written packet. Note: if a send returned MustDrain, no subsequent
    /// packets can be sent without successfully draining first.
    fn drain(&mut self) -> Result<(), SendError> {
        Ok(())
    }

    /// Returns whether this port has data to drain. If it does, `drain()` must succeed before
    /// a new packet can be sent out. If it doesn't, a new packet can be sent immediately.
    fn has_data_to_drain(&self) -> bool {
        false
    }

    /// Set data rate. Note that this purely changes the rate on the host computer. All RPC
    /// interactions with the device to change rate must be done manually.
    fn set_rate(&mut self, _rate: u32) -> Result<(), RateError> {
        Err(RateError::Unsupported)
    }

    /// Get the `RateInfo` for this port. If None, `set_rate()` is unsupported.
    fn rate_info(&self) -> Option<RateInfo> {
        None
    }

    /// If specified, a packet should be sent on this port at most this long after the last send.
    /// `tio::port::Port` will automatically insert a Heartbeat to satisfy this requirement.
    fn max_send_interval(&self) -> Option<Duration> {
        None
    }
}

enum AddrFamilyRestrict {
    V4,
    V6,
    Either,
}

static TIO_DEFAULT_PORT: u16 = 7855;

fn find_addr(addr: &str, family: AddrFamilyRestrict) -> Result<SocketAddr, io::Error> {
    // If the port is missing, append the default. It would
    // be possible to determine if it's needed, but it's simpler
    // to try to parse as-is, and if it fails try again with the port.
    let iter = match addr.to_socket_addrs() {
        Ok(iter) => iter,
        Err(err) => {
            let addr = format!("{}:{}", addr, TIO_DEFAULT_PORT);
            match addr.to_socket_addrs() {
                Ok(iter) => iter,
                _ => {
                    return Err(err);
                }
            }
        }
    };
    for sa in iter {
        match sa {
            SocketAddr::V4(_) => {
                if let AddrFamilyRestrict::V6 = family {
                    continue;
                }
            }
            SocketAddr::V6(_) => {
                if let AddrFamilyRestrict::V4 = family {
                    continue;
                }
            }
        }
        return Ok(sa);
    }
    Err(io::Error::new(
        io::ErrorKind::Other,
        "address resolution failed",
    ))
}

enum PacketOrControl {
    Pkt(Packet),
    SetRate(u32),
}

enum ControlResult {
    Success,
    SetRateError(RateError),
}

pub struct Port {
    tx: Option<Box<crossbeam::channel::Sender<PacketOrControl>>>,
    waker: mio::Waker,
    ctl_result: crossbeam::channel::Receiver<ControlResult>,
    rates: Option<RateInfo>,
}

impl Port {
    fn poller_thread<
        RawPortT: RawPort + mio::event::Source,
        RxCallbackT: Fn(Result<Packet, RecvError>) -> io::Result<()>,
    >(
        mut raw_port: RawPortT,
        mut poll: mio::Poll,
        rx: RxCallbackT,
        tx: crossbeam::channel::Receiver<PacketOrControl>,
        ctl_result: crossbeam::channel::Sender<ControlResult>,
    ) {
        // TODO: if anything panics in this thread and unwinds,
        // the proxy thread will notice the channel closure,
        // but always fails to reconnect. It appears that the
        // actual serial port is never closed.
        // It seems that manually calling std::mem::drop(raw_port)
        // before panicking works.
        // Tested on linux, it works without such requirement.
        // Try to figure out why it's happening.
        // Possible solution: panic::catch_unwind around this whole block
        // and manually drop the port??
        use crossbeam::channel::TryRecvError;

        let mut events = mio::Events::with_capacity(1);
        let mut needs_draining = false;

        // This gets set in cases where we ignore tx packets due to
        // the port queue being full.
        let mut needs_tx_queue_check = false;

        poll.registry()
            .register(&mut raw_port, mio::Token(1), mio::Interest::READABLE)
            .expect("mio::Poll raw_port registration failure");

        let mut last_sent = Instant::now();

        'ioloop: loop {
            let timeout = if needs_draining {
                None
            } else if let Some(max_interval) = raw_port.max_send_interval() {
                Some({
                    let mut until_hb = max_interval.saturating_sub(last_sent.elapsed());
                    if until_hb == Duration::ZERO {
                        match raw_port.send(&Packet::make_hb(None)) {
                            Err(SendError::MustDrain) => {
                                needs_draining = true;
                                poll.registry()
                                    .reregister(
                                        &mut raw_port,
                                        mio::Token(1),
                                        mio::Interest::READABLE.add(mio::Interest::WRITABLE),
                                    )
                                    .expect("Writable interest set failed (HB)");
                                continue;
                            }
                            Err(_) => {
                                break;
                            }
                            Ok(_) => {
                                last_sent = Instant::now();
                                until_hb = max_interval;
                            }
                        }
                    }
                    // Note: here we always sleep an additional millisecond, otherwise we just poll
                    // in a loop for one millisecond on some systems when until_hb is above zero
                    // but below 1 ms.
                    until_hb + Duration::from_millis(1)
                })
            } else {
                None
            };

            poll.poll(&mut events, timeout).expect("Poll failed");

            let mut check_tx_channel = false;

            for event in events.iter() {
                match event.token() {
                    mio::Token(0) => {
                        // One or more packets were sent on the tx queue, or the tx queue was closed.
                        if needs_draining {
                            needs_tx_queue_check = true;
                        } else {
                            check_tx_channel = true;
                        }
                    }
                    mio::Token(1) => {
                        if event.is_writable() {
                            if needs_draining {
                                match raw_port.drain() {
                                    Ok(_) => {
                                        needs_draining = false;
                                        poll.registry()
                                            .reregister(
                                                &mut raw_port,
                                                mio::Token(1),
                                                mio::Interest::READABLE,
                                            )
                                            .expect("Readable interest set failed");
                                        last_sent = Instant::now();
                                    }
                                    Err(SendError::MustDrain) => {
                                        // Must keep trying, do nothing
                                    }
                                    Err(_) => {
                                        break 'ioloop;
                                    }
                                }
                            } else {
                                // At the time of writing this with the most current libraries,
                                // under windows the interest appears to always writable if we
                                // can write to the port, regardless of registered interest.
                                // TODO: re-check and/or try to figure this out before release.
                                #[cfg(unix)]
                                panic!("Unexpected writable raw port when not draining");
                            }
                        }
                        // Packet or error available from the device
                        loop {
                            match raw_port.recv() {
                                Ok(pkt) => {
                                    if let Err(_) = rx(Ok(pkt)) {
                                        // RX callback signaled an error, terminate.
                                        break 'ioloop;
                                    }
                                }
                                Err(RecvError::NotReady) => {
                                    break;
                                }
                                Err(e) => {
                                    // Pass error along. Rx callback will determine what to do.
                                    // if it returns an error, break out.
                                    if let Err(_) = rx(Err(e)) {
                                        break 'ioloop;
                                    }
                                }
                            };
                        }
                    }
                    mio::Token(x) => {
                        panic!("Unexpected token {}", x);
                    }
                }
            }

            if !needs_draining && needs_tx_queue_check {
                check_tx_channel = true;
                needs_tx_queue_check = false;
            }

            if check_tx_channel {
                // Dequeue and send to the device port, or break out.
                loop {
                    match tx.try_recv() {
                        Ok(PacketOrControl::Pkt(pkt)) => {
                            match raw_port.send(&pkt) {
                                Err(SendError::MustDrain) => {
                                    needs_draining = true;
                                    poll.registry()
                                        .reregister(
                                            &mut raw_port,
                                            mio::Token(1),
                                            mio::Interest::READABLE.add(mio::Interest::WRITABLE),
                                        )
                                        .expect("Writable interest set failed (TX)");
                                }
                                Err(SendError::Full) => {
                                    // This should never happen. The `RawPort`s will always
                                    // return MustDrain before Full, and the code in the
                                    // ioloop will ensure that a port in that state is
                                    // drained successfully before receiving anything on tx.
                                }
                                Err(_) => {
                                    break 'ioloop;
                                }
                                Ok(_) => {
                                    last_sent = Instant::now();
                                }
                            }
                        }
                        Ok(PacketOrControl::SetRate(rate)) => {
                            if let Err(_) = ctl_result.send(match raw_port.set_rate(rate) {
                                Ok(_) => ControlResult::Success,
                                Err(e) => ControlResult::SetRateError(e),
                            }) {
                                break 'ioloop;
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            break 'ioloop;
                        }
                    }
                }
            }
        }
    }

    pub fn new<
        RawPortT: RawPort + mio::event::Source + Send + 'static,
        RxCallbackT: Fn(Result<Packet, RecvError>) -> io::Result<()> + Send + 'static,
    >(
        raw_port: RawPortT,
        rx: RxCallbackT,
    ) -> io::Result<Port> {
        let rates = raw_port.rate_info();
        let (tx, ttx) = crossbeam::channel::bounded::<PacketOrControl>(32);
        let (ctl_ret_sender, ctl_ret_receiver) = crossbeam::channel::bounded::<ControlResult>(1);
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), mio::Token(0))?;
        thread::spawn(move || {
            Port::poller_thread(raw_port, poll, rx, ttx, ctl_ret_sender);
        });
        io::Result::Ok(Port {
            tx: Some(Box::new(tx)),
            ctl_result: ctl_ret_receiver,
            waker: waker,
            rates: rates,
        })
    }

    pub fn from_url<RXT: Fn(Result<Packet, RecvError>) -> io::Result<()> + Send + 'static>(
        url: &str,
        rx: RXT,
    ) -> io::Result<Port> {
        // Special case: serial ports can be given directly
        #[cfg(unix)]
        if url.starts_with("/dev/") {
            return Port::new(serial::Port::new(url)?, rx);
        }
        #[cfg(windows)]
        if url.starts_with("COM") {
            return Port::new(serial::Port::new(url)?, rx);
        }

        let split_url: Vec<&str> = url.splitn(2, "://").collect();
        match split_url[..] {
            ["serial", port] => Port::new(serial::Port::new(port)?, rx),
            ["tcp", addr] => Port::new(
                tcp::Port::new(&find_addr(addr, AddrFamilyRestrict::Either)?)?,
                rx,
            ),
            ["udp", addr] => Port::new(
                udp::Port::new(&find_addr(addr, AddrFamilyRestrict::Either)?)?,
                rx,
            ),
            ["tcp4", addr] => Port::new(
                tcp::Port::new(&find_addr(addr, AddrFamilyRestrict::V4)?)?,
                rx,
            ),
            ["udp4", addr] => Port::new(
                udp::Port::new(&find_addr(addr, AddrFamilyRestrict::V4)?)?,
                rx,
            ),
            ["tcp6", addr] => Port::new(
                tcp::Port::new(&find_addr(addr, AddrFamilyRestrict::V6)?)?,
                rx,
            ),
            ["udp6", addr] => Port::new(
                udp::Port::new(&find_addr(addr, AddrFamilyRestrict::V6)?)?,
                rx,
            ),
            _ => io::Result::Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid url")),
        }
    }

    pub fn from_mio_stream<
        RXT: Fn(Result<Packet, RecvError>) -> io::Result<()> + Send + 'static,
    >(
        stream: mio::net::TcpStream,
        rx: RXT,
    ) -> io::Result<Port> {
        Port::new(tcp::Port::from_stream(stream)?, rx)
    }

    pub fn from_tcp_stream<
        RXT: Fn(Result<Packet, RecvError>) -> io::Result<()> + Send + 'static,
    >(
        stream: std::net::TcpStream,
        rx: RXT,
    ) -> io::Result<Port> {
        stream.set_nonblocking(true)?;
        Port::from_mio_stream(mio::net::TcpStream::from_std(stream), rx)
    }

    // let (port_rx_sender, port_rx) = rx_channels();
    pub fn rx_channel() -> (
        crossbeam::channel::Sender<Result<Packet, RecvError>>,
        crossbeam::channel::Receiver<Result<Packet, RecvError>>,
    ) {
        crossbeam::channel::bounded::<Result<Packet, RecvError>>(32)
    }

    pub fn rx_to_channel_cb<FullCBT: Fn(Result<Packet, RecvError>) -> () + Send + 'static>(
        rx_send: crossbeam::channel::Sender<Result<Packet, RecvError>>,
        full_cb: FullCBT,
    ) -> impl Fn(Result<Packet, RecvError>) -> io::Result<()> {
        move |rxdata| -> io::Result<()> {
            if let Err(RecvError::Disconnected) = rxdata {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            use crossbeam::channel::TrySendError;
            match rx_send.try_send(rxdata) {
                Err(TrySendError::Full(res)) => {
                    full_cb(res);
                    Ok(())
                }
                Err(e) => {
                    if let TrySendError::Disconnected(_) = e {
                        Err(io::Error::from(io::ErrorKind::BrokenPipe))
                    } else {
                        Err(io::Error::from(io::ErrorKind::Other))
                    }
                }
                Ok(_) => Ok(()),
            }
        }
    }

    pub fn rx_to_channel(
        rx_send: crossbeam::channel::Sender<Result<Packet, RecvError>>,
    ) -> impl Fn(Result<Packet, RecvError>) -> io::Result<()> {
        Port::rx_to_channel_cb(rx_send, |_| {})
    }

    pub fn send(&self, packet: Packet) -> Result<(), SendError> {
        let tx = self.tx.as_ref().expect("Tx channel invalid");
        if let Err(_) = tx.send(PacketOrControl::Pkt(packet)) {
            Err(SendError::Disconnected)
        } else if let Err(_) = self.waker.wake() {
            panic!("Wake failed");
        } else {
            Ok(())
        }
    }

    pub fn rate_info(&self) -> Option<RateInfo> {
        self.rates.clone()
    }

    pub fn set_rate(&self, rate: u32) -> Result<(), RateError> {
        let tx = self.tx.as_ref().expect("Tx channel invalid");
        if let Err(_) = tx.send(PacketOrControl::SetRate(rate)) {
            return Err(RateError::Failed);
        } else if let Err(_) = self.waker.wake() {
            panic!("Wake failed");
        }
        match self.ctl_result.recv().expect("Missing control result") {
            ControlResult::Success => Ok(()),
            ControlResult::SetRateError(err) => Err(err),
        }
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        let mut channel = None;
        std::mem::swap(&mut self.tx, &mut channel);
        drop(channel);
        if let Err(_) = self.waker.wake() {
            panic!("Wake failed");
        }
    }
}

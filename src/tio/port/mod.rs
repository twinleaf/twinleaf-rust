mod iobuf;
mod serial;
mod tcp;
mod udp;

use super::proto::{self, Packet};
use std::io;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum RecvError {
    NotReady,
    Disconnected,
    Protocol(proto::Error),
    IO(io::Error),
}

#[derive(Debug)]
pub enum SendError {
    MustDrain,
    Full,
    Disconnected,
    IO(io::Error),
}

#[derive(Debug)]
pub enum RateError {
    Unsupported,
    InvalidRate,
    Failed,
}

#[derive(Clone)]
pub struct RateInfo {
    pub default_bps: u32,
    pub min_bps: u32,
    pub max_bps: u32,
}

pub trait RawPort {
    // Returns a packet without blocking, or RecvError::NotReady if one is not available.
    // For all the other error values, the port should be torn down, and possibly recreated.
    fn recv(&mut self) -> Result<Packet, RecvError>;

    // Attempts to send a packet. If it doesn't return Ok:
    // - if it returned MustDrain, the packet was sent partially, and must be drained manually via drain()
    // - if it returned Full, the last packet written was MustDrain and it hasn't been drained yet
    // - for all other errors, the appropriate action is to tear down this port and recreate.
    fn send(&mut self, pkt: &Packet) -> Result<(), SendError>;

    // Drain partially written packet. Note: if a send returned MustDrain, no subsequent
    // packets can be sent without successfully draining first.
    fn drain(&mut self) -> Result<(), SendError> {
        Ok(())
    }
    fn has_data_to_drain(&self) -> bool {
        false
    }

    // Set data rate. Note that this purely changes the rate on the host computer. All RPC
    // interactions with the device to change rate must be done manually.
    fn set_rate(&mut self, _rate: u32) -> Result<(), RateError> {
        Err(RateError::Unsupported)
    }
    fn rate_info(&self) -> Option<RateInfo> {
        None
    }

    // If not ZERO, a packet should be sent on this port at most this long after the last send.
    fn max_send_interval(&self) -> Option<Duration> {
        None
    }
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
        use crossbeam::channel::TryRecvError;

        let mut events = mio::Events::with_capacity(1);
        let mut needs_draining = false;

        poll.registry()
            .register(&mut raw_port, mio::Token(1), mio::Interest::READABLE)
            .unwrap();

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
                                    .unwrap();
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

            poll.poll(&mut events, timeout).unwrap();

            for event in events.iter() {
                match event.token() {
                    mio::Token(0) => {
                        // One or more packets were sent on the tx queue, or the tx queue was closed.
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
                                                    mio::Interest::READABLE
                                                        .add(mio::Interest::WRITABLE),
                                                )
                                                .unwrap();
                                        }
                                        Err(SendError::Full) => {
                                            // Keep draining tx packets even when we can't send them.
                                            // TODO: have some way to signal this condition??
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
                    mio::Token(1) => {
                        if event.is_writable() {
                            if !needs_draining {
                                panic!("Unexpected writable raw port when not draining");
                            }
                            match raw_port.drain() {
                                Ok(_) => {
                                    needs_draining = false;
                                    poll.registry()
                                        .reregister(
                                            &mut raw_port,
                                            mio::Token(1),
                                            mio::Interest::READABLE,
                                        )
                                        .unwrap();
                                    last_sent = Instant::now();
                                }
                                Err(SendError::MustDrain) => {
                                    // Must keep trying, do nothing
                                }
                                Err(_) => {
                                    break 'ioloop;
                                }
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
            ["tcp", addr] => Port::new(tcp::Port::new(&tio_addr(addr).unwrap())?, rx),
            ["udp", addr] => Port::new(udp::Port::new(&tio_addr(addr).unwrap())?, rx),
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

    // TODO: review channel stuff

    // let (port_rx_sender, port_rx) = rx_channels();
    pub fn rx_channel() -> (
        crossbeam::channel::Sender<Result<Packet, RecvError>>,
        crossbeam::channel::Receiver<Result<Packet, RecvError>>,
    ) {
        crossbeam::channel::bounded::<Result<Packet, RecvError>>(32)
    }

    pub fn rx_to_channel(
        rx_send: crossbeam::channel::Sender<Result<Packet, RecvError>>,
    ) -> impl Fn(Result<Packet, RecvError>) -> io::Result<()> {
        move |rxdata| -> io::Result<()> {
            use crossbeam::channel::TrySendError;
            match rx_send.try_send(rxdata) {
                Err(TrySendError::Full(_)) => {
                    //if rx_drop_count == 0 {
                    //println!("Dropping rx packets.");
                    //}
                    //rx_drop_count += 1;
                    Ok(())
                }
                Err(e) => {
                    if let TrySendError::Disconnected(_) = e {
                        Err(io::Error::from(io::ErrorKind::BrokenPipe))
                    } else {
                        Err(io::Error::from(io::ErrorKind::Other))
                    }
                }
                Ok(_) => {
                    /*
                    if rx_drop_count > 0 {
                        println!(
                            "Resumed RX. Dropped {} packets.",
                            rx_drop_count
                        );
                        rx_drop_count = 0;
                    }
                    */
                    //println!("{}", queued);
                    Ok(())
                }
            }
        }
    }

    pub fn send(&self, packet: Packet) -> Result<(), SendError> {
        if let Err(_) = self.tx.as_ref().unwrap().send(PacketOrControl::Pkt(packet)) {
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
        if let Err(_) = self
            .tx
            .as_ref()
            .unwrap()
            .send(PacketOrControl::SetRate(rate))
        {
            return Err(RateError::Failed);
        } else if let Err(_) = self.waker.wake() {
            panic!("Wake failed");
        }
        match self.ctl_result.recv().unwrap() {
            ControlResult::Success => Ok(()),
            ControlResult::SetRateError(err) => Err(err), // _ => {panic!("Unexpected control result"); }
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

// TODO: temp

use std::net::{SocketAddr, ToSocketAddrs};

fn tio_addr(addr: &str) -> Result<SocketAddr, io::Error> {
    let mut iter = match addr.to_socket_addrs() {
        Ok(iter) => iter,
        Err(err) => {
            let addr = format!("{}:7855", addr);
            match addr.to_socket_addrs() {
                Ok(iter) => iter,
                _ => {
                    return Err(err);
                }
            }
        }
    };
    // TODO: restructure this function. should offer a way to try both if one fails, at least for TCP??
    match iter.next() {
        Some(sa) => Ok(sa),
        None => Err(io::Error::new(
            io::ErrorKind::Other,
            "address resolution failed",
        )),
    }
}

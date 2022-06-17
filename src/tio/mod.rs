pub mod proto;
pub mod serial;
pub mod tcp;
pub mod udp;

pub use proto::Packet;

use std::io;
use std::thread;
use std::time::{Duration, Instant};

use std::str::FromStr; // for SocketAddr::from_str

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
    Failed(io::Error),
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

    // Reset data rate to default.
    fn reset_rate(&mut self) -> Result<(), RateError> {
        Err(RateError::Unsupported)
    }
    fn has_settable_rate(&self) -> bool {
        false
    }

    // If not ZERO, a packet should be sent on this port at most this long after the last send.
    fn max_send_interval(&self) -> Option<Duration> {
        None
    }
}

const IOBUF_SIZE: usize = 4096;

struct IOBuf {
    buf: [u8; IOBUF_SIZE],
    start: usize,
    end: usize,
}

impl IOBuf {
    fn new() -> IOBuf {
        IOBuf {
            buf: [0; IOBUF_SIZE],
            start: 0,
            end: 0,
        }
    }

    fn compact(&mut self) {
        if self.start != 0 {
            let len = self.end - self.start;
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = len;
        }
    }

    fn refill<T: io::Read>(&mut self, reader: &mut T) -> Result<(), RecvError> {
        self.compact();
        match reader.read(&mut self.buf[self.end..]) {
            Ok(size) => {
                if size > 0 {
                    // TODO: how does this work with windows timing out?
                    // check that it errs below and change this code accordingly.
                    self.end += size;
                    Ok(())
                } else {
                    Err(RecvError::Disconnected)
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    Err(RecvError::NotReady)
                } else {
                    Err(RecvError::IO(e))
                }
            }
        }
    }

    fn add_data(&mut self, data: &[u8]) -> Result<(), usize> {
        self.compact();
        let copy_size = std::cmp::min(IOBUF_SIZE - self.end, data.len());
        &self.buf[self.end..self.end + copy_size].copy_from_slice(&data[0..copy_size]);
        if copy_size == data.len() {
            Ok(())
        } else {
            Err(copy_size)
        }
    }

    fn drain<T: io::Write>(&mut self, writer: &mut T) -> Result<(), SendError> {
        if self.end > self.start {
            match writer.write(&self.buf[self.start..self.end]) {
                Ok(size) => {
                    self.start += size;
                    if self.start == self.end {
                        Ok(())
                    } else {
                        Err(SendError::MustDrain)
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        Err(SendError::MustDrain)
                    } else {
                        Err(SendError::IO(e))
                    }
                }
            }
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) {
        self.start = 0;
        self.end = 0;
    }
}

pub struct TioPort {
    thd: thread::JoinHandle<()>,
    tx: crossbeam::channel::Sender<Option<Packet>>,
    // TODO: not public
    pub rx: crossbeam::channel::Receiver<Result<Packet, RecvError>>, // TODO: via Fn()
    waker: mio::Waker,
}

impl TioPort {
    fn poller_thread<T: RawPort + mio::event::Source>(
        mut port: T,
        mut poll: mio::Poll,
        rx: crossbeam::channel::Sender<Result<Packet, RecvError>>,
        tx: crossbeam::channel::Receiver<Option<Packet>>,
    ) {
        use crossbeam::channel::TrySendError;
        let mut events = mio::Events::with_capacity(1);

        poll.registry()
            .register(&mut port, mio::Token(1), mio::Interest::READABLE)
            .unwrap();

        let mut last_sent = Instant::now();
        let mut rx_drop_count: usize = 0;

        'ioloop: loop {
            let timeout = if let Some(max_interval) = port.max_send_interval() {
                Some({
                    let mut until_hb = max_interval.saturating_sub(last_sent.elapsed());
                    if until_hb == Duration::ZERO {
                        port.send(&Packet::make_hb(None));
                        last_sent = Instant::now();
                        until_hb = max_interval;
                    }
                    // Note: here we always sleep an additional millisecond, otherwise we just poll in a loop for one millisecond on some systems when until_hb is above zero but below 1 ms.
                    until_hb + Duration::from_millis(1)
                })
            } else {
                None
            };

            poll.poll(&mut events, timeout).unwrap();

            //println!("POLL {:?}", until_hb);

            for event in events.iter() {
                match event.token() {
                    mio::Token(0) => {
                        for pkt in tx.try_iter() {
                            match pkt {
                                Some(pkt) => {
                                    port.send(&pkt);
                                }
                                None => {
                                    break 'ioloop;
                                }
                            }
                        }
                    }
                    mio::Token(1) => {
                        loop {
                            match port.recv() {
                                Ok(pkt) => {
                                    match rx.try_send(Ok(pkt)) {
                                        Err(TrySendError::Full(_)) => {
                                            if rx_drop_count == 0 {
                                                println!("Dropping rx packets.");
                                            }
                                            rx_drop_count += 1;
                                        }
                                        Err(_) => {
                                            // TODO
                                            break 'ioloop;
                                        }
                                        Ok(_) => {
                                            if rx_drop_count > 0 {
                                                println!(
                                                    "Resumed RX. Dropped {} packets.",
                                                    rx_drop_count
                                                );
                                                rx_drop_count = 0;
                                            }
                                            //println!("{}", queued);
                                        }
                                    }
                                }
                                Err(RecvError::NotReady) => {
                                    break;
                                }
                                Err(e) => {
                                    rx.send(Err(e));
                                    println!("rx error");
                                    break 'ioloop;
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

    pub fn new<T: RawPort + mio::event::Source + Send + 'static>(
        raw_port: T,
    ) -> io::Result<TioPort> {
        let (tx, ttx) = crossbeam::channel::bounded::<Option<Packet>>(32);
        let (trx, rx) = crossbeam::channel::bounded::<Result<Packet, RecvError>>(32);
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), mio::Token(0))?;
        io::Result::Ok(TioPort {
            thd: thread::spawn(move || {
                TioPort::poller_thread(raw_port, poll, trx, ttx);
            }),
            tx: tx,
            rx: rx,
            waker: waker,
        })
    }

    pub fn from_url(url: &str) -> io::Result<TioPort> {
        // Special case: serial ports can be given directly
        #[cfg(unix)]
        if url.starts_with("/dev/") {
            return TioPort::new(serial::Port::new(url)?);
        }
        #[cfg(windows)]
        if url.starts_with("COM") {
            return TioPort::new(serial::Port::new(url)?);
        }

        let split_url: Vec<&str> = url.splitn(2, "://").collect();
        match split_url[..] {
            ["serial", port] => TioPort::new(serial::Port::new(port)?),
            ["tcp", addr] => TioPort::new(tcp::Port::new(&tio_addr(addr).unwrap())?),
            //["udp",addr] => { TioPort::new(udp::Port::new(addr)?) }
            _ => io::Result::Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid url")),
        }
    }

    pub fn send(&self, packet: Packet) {
        self.tx.send(Some(packet));
        self.waker.wake();
    }

    pub fn recv(&self) -> Result<Packet, RecvError> {
        self.rx.recv().unwrap() // TODO
    }

    //    fn recv_timeout(&self, timeout: Duration) -> Result<TioPacket, ReadError> {
    //    }

    /*
    fn iter(&self) -> TioPortIterator {
    }

    fn iter_timeout(&self, timeout: Duration) -> TioPortIterator {
    }
    */
}

impl Drop for TioPort {
    fn drop(&mut self) {
        self.tx.send(None);
        self.waker.wake();
    }
}

use std::net::{SocketAddr, ToSocketAddrs};

pub fn tio_addr(addr: &str) -> Result<SocketAddr, io::Error> {
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
    match iter.next() {
        Some(sa) => Ok(sa),
        None => Err(io::Error::new(
            io::ErrorKind::Other,
            "address resolution failed",
        )),
    }
}

pub fn log_msg(desc: &str, what: &Packet) -> String {
    format!("{:?} {}  -- {:?}", Instant::now(), desc, what.payload)
}

pub fn log_msg2(desc: &str) {
    println!("{:?} {}", Instant::now(), desc)
}

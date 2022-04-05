pub mod proto;
pub mod serial;
pub mod tcp;
pub mod udp;

pub use proto::Packet;

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
    IO(io::Error),
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

    // returns true: should refill again once done
    fn refill<T: io::Read>(&mut self, reader: &mut T) -> Result<bool, RecvError> {
        if self.start != 0 {
            let len = self.end - self.start;
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = len;
        }
        //println!("REFILL READ: {} {}", self.start, self.end);
        match reader.read(&mut self.buf[self.end..]) {
            Ok(size) => {
                if size > 0 {
                    self.end += size;
                    Ok(self.end == IOBUF_SIZE)
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

    /*
    fn drain(&mut self, writer: &mut dyn io::Write) -> Result<bool, io::Error> {
        if self.end > 0 {
            match writer.write(&self.buf[self.start..self.end]) {
                _ => { panic!("TODO"); }
            }
        }
        Ok(true)
    }
    */
}

pub trait RawPort {
    fn recv(&mut self) -> Result<Packet, RecvError>;
    fn send(&mut self, pkt: &Packet) -> Result<(), SendError>;
    fn drain(&self) -> Result<(), SendError>;
}

pub struct TioPort {
    thd: thread::JoinHandle<()>,
    tx: crossbeam::channel::Sender<Option<Packet>>,
    // TODO: not public
    pub rx: crossbeam::channel::Receiver<Result<Packet,RecvError>>, // TODO: via Fn()
    waker: mio::Waker,
}

impl TioPort {
    fn poller_thread<T: RawPort + mio::event::Source>(
        mut port: T,
        mut poll: mio::Poll,
        rx: crossbeam::channel::Sender<Result<Packet,RecvError>>,
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
            let mut until_hb = Duration::from_millis(100).saturating_sub(last_sent.elapsed());
            if until_hb == Duration::ZERO {
                port.send(&Packet::make_hb(None));
                last_sent = Instant::now();
                until_hb = Duration::from_millis(100);
            }
            // Note: here we always sleep an additional millisecond, otherwise we just poll in a loop for one millisecond on some systems when until_hb is above zero but below 1 ms.
            poll.poll(&mut events, Some(until_hb+Duration::from_millis(1))).unwrap();

            //println!("POLL {:?}", until_hb);
            
            for event in events.iter() {
                match event.token() {
                    mio::Token(0) => {
                        for pkt in tx.try_iter() {
                            match pkt {
                                Some(pkt) => { port.send(&pkt); }
                                None => { break 'ioloop; }
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
                                                println!("Resumed RX. Dropped {} packets.", rx_drop_count);
                                                rx_drop_count = 0;
                                            }
                                        }
                                    }
                                }
                                Err(RecvError::NotReady)  => { break; }
                                Err(e) => { rx.send(Err(e)); println!("rx error"); break 'ioloop;}
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
    ) -> Result<TioPort, io::Error> {
        let (tx, ttx) = crossbeam::channel::bounded::<Option<Packet>>(10);
        let (trx, rx) = crossbeam::channel::bounded::<Result<Packet,RecvError>>(10);
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

    pub fn send(&self, packet: Packet) {
        self.tx.send(Some(packet));
        self.waker.wake();
    }

    pub fn recv(&self) -> Result<Packet,RecvError> {
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

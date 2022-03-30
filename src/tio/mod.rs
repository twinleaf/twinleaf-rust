use mio_serial::SerialPortBuilderExt;
use packet::*;
use std::io;
use std::io::Write;
use std::thread;

#[derive(Debug)]
enum ReadResult {
    Packet(Vec<u8>),
    //    Text(String),
    WouldBlock,
}

#[derive(Debug)]
enum ReadError {
    IO(io::Error),
    //    Encoding(Vec<u8>),
    //    CRC(Vec<u8>),
    //    Validation(Vec<u8>),
}

#[derive(Debug)]
enum WriteResult {
    Ok,
    //    MustDrain,
}

#[derive(Debug)]
enum WriteError {
    //    IO(io::Error),
//    Full,
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

    fn refill<T: io::Read>(&mut self, reader: &mut T) -> Result<bool, io::Error> {
        if self.start != 0 {
            let len = self.end - self.start;
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = len;
        }
        //println!("REFILL READ: {} {}", self.start, self.end);
        match reader.read(&mut self.buf[self.end..]) {
            Ok(size) => {
                self.end += size;
                Ok(true)
            } // TODO: Ok(0) == closed
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
            _ => {
                panic!("REFILL ERROR");
            } // TODO
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

trait TioRawPort {
    fn recv(&mut self) -> Result<ReadResult, ReadError>;
    fn send(&mut self, pkt: &[u8]) -> Result<WriteResult, WriteError>;
    fn drain(&self) -> Result<WriteResult, WriteError>;
}

pub struct TioSerial {
    port: mio_serial::SerialStream, //Box<dyn mio_serial::SerialPort>,
    //default_bitrate: u32,
    rxbuf: IOBuf,
}

impl TioSerial {
    pub fn new(port_name: &str) -> Result<TioSerial, io::Error> {
        Ok(TioSerial {
            port: mio_serial::new(port_name, 115200).open_native_async()?,
            rxbuf: IOBuf::new(),
        })
    }

    fn recv_buffered(&mut self) -> Result<ReadResult, ReadError> {
        let buf = &self.rxbuf.buf;
        let start = &mut self.rxbuf.start;
        let end = self.rxbuf.end;
        let inbuf = end - *start;
        let mut pkt = Vec::<u8>::new();
        let mut esc = false;
        let mut offset = *start;
        while offset < end {
            if buf[offset] == 0xC0 {
                *start = offset + 1;
                // TODO: validation
                if pkt.len() >= 8 {
                    let len = 4 + (pkt[1] & 0xF) as usize + pkt[2] as usize;
                    if pkt.len() == (len + 4) {
                        pkt.truncate(pkt.len() - 4);
                        return Ok(ReadResult::Packet(pkt));
                    }
                }
                pkt.truncate(0);
            }
            if esc {
                if buf[offset] == 0xDC {
                    pkt.push(0xC0);
                } else {
                    pkt.push(0xDB);
                }
                esc = false;
            } else {
                if buf[offset] == 0xDB {
                    esc = true;
                } else {
                    pkt.push(buf[offset]);
                }
            }
            offset += 1;
        }
        Ok(ReadResult::WouldBlock)
    }
}

impl TioRawPort for TioSerial {
    fn recv(&mut self) -> Result<ReadResult, ReadError> {
        let res = self.recv_buffered();
        if let Ok(ReadResult::WouldBlock) = res {
            self.rxbuf.refill(&mut self.port);
            self.recv_buffered()
        } else {
            res
        }
    }

    fn send(&mut self, pkt: &[u8]) -> Result<WriteResult, WriteError> {
        use crc::{Crc, CRC_32_ISO_HDLC};
        let CRC32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let mut encoded = vec![0xC0u8];
        for byte in [pkt, &CRC32.checksum(pkt).to_le_bytes()[..]].concat() {
            match byte {
                0xC0 => {
                    encoded.push(0xDB);
                    encoded.push(0xDC);
                }
                0xDB => {
                    encoded.push(0xDB);
                    encoded.push(0xDD);
                }
                any => {
                    encoded.push(any);
                }
            }
        }
        encoded.push(0xC0);
        match self.port.write(&encoded) {
            Ok(_size) => Ok(WriteResult::Ok), // TODO: check size and put in iobuf
            _ => {
                panic!("TODO");
            }
        }
    }

    fn drain(&self) -> Result<WriteResult, WriteError> {
        Ok(WriteResult::Ok)
    }
}

impl mio::event::Source for TioSerial {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.port.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.port.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.port.deregister(registry)
    }
}

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
    match iter.next() {
        Some(sa) => Ok(sa),
        None => Err(io::Error::new(
            io::ErrorKind::Other,
            "address resolution failed",
        )),
    }
}

use mio::net::TcpStream;

pub struct TioTCP {
    stream: TcpStream,
    rxbuf: IOBuf,
    //    txbuf: IOBuf,
}

impl TioTCP {
    pub fn new(address: &SocketAddr) -> Result<TioTCP, io::Error> {
        let stream = TcpStream::connect(*address)?;
        Ok(TioTCP {
            stream: stream,
            rxbuf: IOBuf::new(),
            //txbuf: IOBuf::new(),
        })
    }

    fn recv_buffered(&mut self) -> Result<ReadResult, ReadError> {
        let buf = &self.rxbuf.buf;
        let start = &mut self.rxbuf.start;
        let end = self.rxbuf.end;
        let inbuf = end - *start;
        if inbuf < 4 {
            Ok(ReadResult::WouldBlock)
        } else {
            let payload_len = (buf[*start + 2] as usize) + (buf[*start + 3] as usize) * 256;
            let routing_size = (buf[*start + 1] & 0xF) as usize;
            let packet_len = 4 + payload_len + routing_size;
            if inbuf < packet_len {
                Ok(ReadResult::WouldBlock)
            } else {
                let pstart = *start;
                *start += packet_len;
                Ok(ReadResult::Packet(buf[pstart..*start].to_vec()))
            }
        }
    }
}

impl TioRawPort for TioTCP {
    fn recv(&mut self) -> Result<ReadResult, ReadError> {
        let res = self.recv_buffered();
        if let Ok(ReadResult::WouldBlock) = res {
            self.rxbuf.refill(&mut self.stream);
            self.recv_buffered()
        } else {
            res
        }
    }

    fn send(&mut self, pkt: &[u8]) -> Result<WriteResult, WriteError> {
        match self.stream.write(pkt) {
            Ok(_size) => Ok(WriteResult::Ok), // TODO: check size and put in iobuf
            _ => {
                panic!("TODO");
            }
        }
    }

    fn drain(&self) -> Result<WriteResult, WriteError> {
        Ok(WriteResult::Ok)
    }
}

impl mio::event::Source for TioTCP {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.stream.deregister(registry)
    }
}

use mio::net::UdpSocket;

pub struct TioUDP {
    sock: UdpSocket,
}

impl TioUDP {
    pub fn new(address: &SocketAddr) -> Result<TioUDP, io::Error> {
        let sock = UdpSocket::bind("0.0.0.0:0".parse().unwrap())?;
        sock.connect(*address)?;
        Ok(TioUDP { sock })
    }
}

impl TioRawPort for TioUDP {
    fn recv(&mut self) -> Result<ReadResult, ReadError> {
        let mut buf = [0u8; 1024];
        let size = match self.sock.recv(&mut buf) {
            Ok(s) => s,
            Err(err) => {
                return Err(ReadError::IO(err));
            }
        };
        if size < 4 {
            Ok(ReadResult::WouldBlock)
        } else {
            let payload_len = (buf[2] as usize) + (buf[3] as usize) * 256;
            let routing_size = (buf[1] & 0xF) as usize;
            if size == 4 + payload_len + routing_size {
                Ok(ReadResult::Packet(buf[..size].to_vec()))
            } else {
                Ok(ReadResult::WouldBlock)
            }
        }
    }

    fn send(&mut self, pkt: &[u8]) -> Result<WriteResult, WriteError> {
        match self.sock.send(pkt) {
            Ok(_size) => Ok(WriteResult::Ok), // TODO
            _ => {
                panic!("TODO");
            }
        }
    }

    fn drain(&self) -> Result<WriteResult, WriteError> {
        Ok(WriteResult::Ok)
    }
}

impl mio::event::Source for TioUDP {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.sock.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.sock.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.sock.deregister(registry)
    }
}

struct TioPort {
    thd: thread::JoinHandle<()>,
    tx: crossbeam::channel::Sender<Vec<u8>>,
    rx: crossbeam::channel::Receiver<Vec<u8>>, // TODO: via Fn()
    waker: mio::Waker,
}

use std::time::{Duration, Instant};

impl TioPort {
    fn poller_thread<T: TioRawPort + mio::event::Source>(
        mut port: T,
        mut poll: mio::Poll,
        rx: crossbeam::channel::Sender<Vec<u8>>,
        tx: crossbeam::channel::Receiver<Vec<u8>>,
    ) {
        let mut events = mio::Events::with_capacity(1);

        poll.registry()
            .register(&mut port, mio::Token(1), mio::Interest::READABLE)
            .unwrap();

        let mut last_sent = Instant::now();
        loop {
            let mut until_hb = Duration::from_millis(100).saturating_sub(last_sent.elapsed());
            if until_hb == Duration::ZERO {
                port.send(&[5u8, 0, 0, 0]);
                last_sent = Instant::now();
                until_hb = Duration::from_millis(100);
            }
            poll.poll(&mut events, Some(until_hb)).unwrap();

            for event in events.iter() {
                match event.token() {
                    mio::Token(0) => {
                        for pkt in tx.try_iter() {
                            port.send(&pkt);
                        }
                    }
                    mio::Token(1) => {
                        while let Ok(ReadResult::Packet(pkt)) = port.recv() {
                            rx.send(pkt);
                        }
                    }
                    mio::Token(x) => {
                        panic!("Unexpected token {}", x);
                    }
                }
            }
        }
    }

    fn new<T: TioRawPort + mio::event::Source + Send + 'static>(
        raw_port: T,
    ) -> Result<TioPort, io::Error> {
        let (tx, ttx) = crossbeam::channel::bounded::<Vec<u8>>(10);
        let (trx, rx) = crossbeam::channel::bounded::<Vec<u8>>(10);
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

    fn send(&self, packet: Vec<u8>) {
        self.tx.send(packet);
        self.waker.wake();
    }

    fn recv(&self) -> Vec<u8> {
        self.rx.recv().unwrap()
    }
}

pub mod packet;

pub fn stub() {
    println!("TIO Module");
    //let port = TioPort::new(TioSerial::new("/dev/ttyUSB0").unwrap()).unwrap();
    //let port = TioPort::new(TioTCP::new(&tio_addr("localhost").unwrap()).unwrap()).unwrap();
    let port = TioPort::new(TioUDP::new(&tio_addr("tio-SYNC8.local").unwrap()).unwrap()).unwrap();
    let mut send_counter = 0u32;
    while let Ok(pkt) = TioPacket::deserialize(&port.recv()) {
        let mut routing = pkt
            .routing
            .iter()
            .fold(String::new(), |acc, hop| acc + &format!("/{}", hop));
        if routing.len() == 0 {
            routing = "/".to_string();
            match send_counter {
                20 => {
                    port.send(TioPacket::make_rpc("dev.desc".to_string(), &[]).serialize());
                    send_counter += 1;
                }
                21 => {}
                _ => send_counter += 1,
            }
        }
        match pkt.payload {
            TioData::Stream(sample) => {
                println!(
                    "{}: Stream {}: sample {} {:?}",
                    routing, sample.stream_id, sample.sample_n, sample.payload
                );
            }
            TioData::RpcReply(rep) => {
                let human_readable = if let Ok(s) = std::str::from_utf8(&rep.reply) {
                    s
                } else {
                    ""
                };
                println!("Reply: **{}** {:?}", human_readable, rep.reply);
                break;
            }
            TioData::RpcRequest(_) => {
                println!("Request???");
            }
            TioData::Unknown(unk) => {
                println!(
                    "Unknown packet, type {} size {}",
                    unk.msg_id,
                    unk.payload.len()
                );
            }
        }
    }
}

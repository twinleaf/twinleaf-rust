//! A fake device behind a TCP port on this machine.

#![cfg_attr(all(test, turmoil), allow(dead_code))]

use super::WAIT;
use crate::proto::data as wire;
use crate::proto::rpc::Method;
use crate::proto::sync::Epoch;
use crate::proto::{ColumnId, DeviceRoute, PacketType, SegmentId, SessionId, StreamId};
use crate::tio::packet::{Packet, Payload};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What a fake device's threads share with the test.
#[derive(Default)]
struct DeviceState {
    accepted: AtomicUsize,
    /// How many connections are open.
    open: AtomicUsize,
    /// A clone of each open connection, by the order it was accepted in.
    live: Mutex<Vec<(usize, TcpStream)>>,
    /// Set to refuse a connection while another is open.
    exclusive: AtomicBool,
    /// How many connections were refused.
    refused: AtomicUsize,
    /// Bumped to drop every connection.
    generation: AtomicUsize,
    /// Packets for a connection to send.
    outbox: Mutex<Vec<Packet>>,
    /// Every packet the device received.
    received: Mutex<Vec<Packet>>,
    /// How long it takes to answer `dev.name`.
    reply_delay: Mutex<Duration>,
    /// The hash of the device's RPC table, one no other fake device shares.
    table_hash: u32,
    stop: AtomicBool,
}

/// A device behind a TCP port on this machine. Each connection gets a
/// heartbeat about every 300 ms, and answers to `dev.name`, `dev.metadata`
/// and the calls that load its RPC table and read its one setting.
pub(crate) struct FakeDevice {
    /// The URL it listens at.
    pub(crate) url: String,
    state: Arc<DeviceState>,
}

impl FakeDevice {
    /// Starts a fake device on a free port of `127.0.0.1`.
    pub(crate) fn start() -> FakeDevice {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let url = format!("tcp://{address}");
        listener.set_nonblocking(true).unwrap();
        let state = Arc::new(DeviceState {
            table_hash: (std::process::id() << 16) | u32::from(address.port()),
            ..DeviceState::default()
        });
        let accepting = state.clone();
        std::thread::spawn(move || {
            while !accepting.stop.load(Ordering::SeqCst) {
                let Ok((stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                };
                let id = accepting.accepted.fetch_add(1, Ordering::SeqCst);
                if accepting.exclusive.load(Ordering::SeqCst) && accepting.held() {
                    accepting.refused.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                let serving = accepting.clone();
                // The connection ends once the generation moves on.
                let born = serving.generation.load(Ordering::SeqCst);
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(READ_POLL)).unwrap();
                let clone = stream.try_clone().unwrap();
                crate::chan::lock(&serving.live).push((id, clone));
                serving.open.fetch_add(1, Ordering::SeqCst);
                std::thread::spawn(move || {
                    serve_device(stream, &serving, born);
                    serving.open.fetch_sub(1, Ordering::SeqCst);
                    crate::chan::lock(&serving.live).retain(|(live, _)| *live != id);
                });
            }
        });
        FakeDevice { url, state }
    }

    /// Returns how many connections the device has accepted.
    pub(crate) fn accepted(&self) -> usize {
        self.state.accepted.load(Ordering::SeqCst)
    }

    /// Returns how many of the device's connections are open. A connection
    /// counts until the device sees it close.
    pub(crate) fn connections(&self) -> usize {
        self.state.open.load(Ordering::SeqCst)
    }

    /// Refuses each new connection while an earlier one is open, as a serial
    /// port does. The device closes a refused connection at once.
    pub(crate) fn exclusive(&self) {
        self.state.exclusive.store(true, Ordering::SeqCst);
    }

    /// Returns how many connections the device refused.
    pub(crate) fn refused(&self) -> usize {
        self.state.refused.load(Ordering::SeqCst)
    }

    /// Closes every connection, as a device that resets does.
    pub(crate) fn drop_connections(&self) {
        self.state.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns how many calls of the RPC `name` the device has received.
    pub(crate) fn calls(&self, name: &str) -> usize {
        let received = crate::chan::lock(&self.state.received);
        let called = |pkt: &&Packet| match pkt.payload() {
            Payload::RpcRequest(request) => request.method == Method::ByName(name.as_bytes()),
            _ => false,
        };
        received.iter().filter(called).count()
    }

    /// Delays each answer to `dev.name` by `delay`. Heartbeats continue
    /// meanwhile.
    pub(crate) fn delay_replies(&self, delay: Duration) {
        *crate::chan::lock(&self.state.reply_delay) = delay;
    }

    /// Sends `pkt` on the device's connection.
    pub(crate) fn send(&self, pkt: Packet) {
        crate::chan::lock(&self.state.outbox).push(pkt);
    }

    /// Blocks until the device has received `count` packets of type `ptype`,
    /// and returns them. Panics if they do not arrive within [`WAIT`].
    pub(crate) fn expect_received(&self, ptype: PacketType, count: usize) -> Vec<Packet> {
        let deadline = Instant::now() + WAIT;
        loop {
            let received: Vec<_> = crate::chan::lock(&self.state.received)
                .iter()
                .filter(|pkt| pkt.ptype() == ptype)
                .cloned()
                .collect();
            if received.len() >= count {
                return received;
            }
            assert!(
                Instant::now() < deadline,
                "the device received {} of {count} packets",
                received.len()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for FakeDevice {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::SeqCst);
    }
}

impl DeviceState {
    /// Returns `true` if a connection is open that its peer has not closed.
    fn held(&self) -> bool {
        let live = crate::chan::lock(&self.live);
        live.iter().any(|(_, stream)| !peer_closed(stream))
    }
}

/// How long a connection's thread waits for each read.
const READ_POLL: Duration = Duration::from_millis(20);

/// Returns `true` if the peer of `stream` has closed it. Waits for the
/// device to read what arrived before the close.
fn peer_closed(stream: &TcpStream) -> bool {
    for _ in 0..10 {
        match stream.peek(&mut [0]) {
            Ok(0) => return true,
            Ok(_) => std::thread::sleep(Duration::from_millis(10)),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return false
            }
            Err(_) => return true,
        }
    }
    false
}

fn serve_device(mut stream: TcpStream, state: &DeviceState, born: usize) {
    stream.set_nonblocking(false).unwrap();
    stream.set_read_timeout(Some(READ_POLL)).unwrap();
    let heartbeat = Packet::heartbeat(DeviceRoute::root());
    let mut next_beat = Instant::now();
    let mut received = Vec::new();
    // Each reply to send, and when.
    let mut replies: Vec<(Instant, Packet)> = Vec::new();
    while !state.stop.load(Ordering::SeqCst) && state.generation.load(Ordering::SeqCst) == born {
        if Instant::now() >= next_beat {
            if stream.write_all(heartbeat.as_bytes()).is_err() {
                return;
            }
            next_beat += Duration::from_millis(300);
        }
        let mut outbox: Vec<_> = crate::chan::lock(&state.outbox).drain(..).collect();
        let now = Instant::now();
        outbox.extend(
            replies
                .extract_if(.., |(due, _)| *due <= now)
                .map(|(_, pkt)| pkt),
        );
        for pkt in outbox {
            if stream.write_all(pkt.as_bytes()).is_err() {
                return;
            }
        }
        let mut chunk = [0; 512];
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => received.extend_from_slice(&chunk[..n]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
        while let Ok((pkt, used)) = Packet::from_slice_prefix(&received) {
            received.drain(..used);
            crate::chan::lock(&state.received).push(pkt.clone());
            let Payload::RpcRequest(request) = pkt.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            let due = match name {
                b"dev.name" => Instant::now() + *crate::chan::lock(&state.reply_delay),
                _ => Instant::now(),
            };
            if let Some(value) = answer(name, request.args, state.table_hash) {
                let reply = Packet::rpc_reply(request.id.value(), &value, pkt.route());
                replies.push((due, reply.unwrap()));
            }
        }
    }
}

/// The fake device's RPC table besides `dev.name`, each entry's name and
/// metadata word. It has one readable, announced setting and
/// `settings.version`.
const TABLE: [(&str, u16); 2] = [("test.amplitude", 0xA310), ("settings.version", 0x8140)];

/// Returns the fake device's answer to the RPC `name` with `args`, or `None`
/// for an RPC it leaves unanswered. It describes itself as one stream of one
/// column, in session 7, and its table has the hash `table_hash`.
pub(crate) fn answer(name: &[u8], args: &[u8], table_hash: u32) -> Option<Vec<u8>> {
    Some(match name {
        b"dev.name" => b"FAKE".to_vec(),
        b"rpc.hash" => table_hash.to_le_bytes().to_vec(),
        b"dev.session" | b"settings.version" => 7u32.to_le_bytes().to_vec(),
        b"rpc.listinfo" if args.is_empty() => (TABLE.len() as u16).to_le_bytes().to_vec(),
        b"rpc.listinfo" => {
            let index = u16::from_le_bytes(args.try_into().ok()?);
            let (name, meta) = TABLE.get(usize::from(index))?;
            [&meta.to_le_bytes()[..], name.as_bytes()].concat()
        }
        b"test.amplitude" => vec![1],
        b"dev.metadata" => {
            let mut reply = vec![0u8; wire::MAX_METADATA_REPLY_SIZE];
            let written = description().iter().fold(0, |written, record| {
                written + record.write_reply_frame(&mut reply[written..]).unwrap()
            });
            reply.truncate(written);
            reply
        }
        _ => return None,
    })
}

/// The records the fake device describes itself with, in session 7.
pub(crate) fn description() -> [wire::Metadata<'static>; 4] {
    let stream_id = StreamId::new(1);
    [
        wire::Metadata::Device(wire::Device {
            session: SessionId::new(7),
            n_streams: 1,
            name: "FAKE",
            serial: "F1",
            firmware: "1",
            protocol: 0,
        }),
        wire::Metadata::Stream(wire::Stream {
            stream_id,
            n_columns: 1,
            n_segments: 2,
            sample_size: 4,
            buf_samples: 128,
            name: "field",
        }),
        wire::Metadata::Segment(wire::Segment {
            stream_id,
            segment_id: SegmentId::new(0),
            flags: wire::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "",
            timeref_session: SessionId::new(7),
            start_time: 0,
            sampling_rate: 10,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
        }),
        wire::Metadata::Column(wire::Column {
            stream_id,
            index: ColumnId::new(0),
            data_type: wire::DataType::F32,
            name: "x",
            units: "nT",
            description: "",
        }),
    ]
}

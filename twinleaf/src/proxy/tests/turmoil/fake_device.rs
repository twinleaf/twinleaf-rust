//! The fake device of the turmoil tests, which serves TCP as the firmware
//! does, as `faults` says, and writes what it sees to the [`World`].

use super::{child, faults, World, CHILD_REBOOT_STEP, DEVICE_PORT, TABLE_HASH};
use crate::chan::sleep_until;
use crate::device::tests::fixtures::{description, metadata_records};
use crate::net::{TcpListener, TcpStream};
use crate::proto::rpc::Method;
use crate::proto::DeviceRoute;
use crate::proxy::tests::fixtures::answer;
use crate::tio::packet::{Packet, Payload};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

/// One boot of the device.
struct Boot {
    /// How many boots came before it.
    index: u32,
    /// Its session, 7 on the first boot and one more on each later one.
    session: u32,
    /// When it started.
    born: Instant,
    /// One more than the highest sample number it has sent on any
    /// connection.
    unsent: AtomicU32,
}

impl Boot {
    /// How many times the device at `route` has rebooted in this boot, none
    /// at the root.
    fn reboots(route: DeviceRoute, world: &World) -> u32 {
        match route == DeviceRoute::root() {
            true => 0,
            false => world.child_reboots.load(Ordering::SeqCst),
        }
    }

    /// The session of the device at `route` now. The root's is the boot's,
    /// and the hub child's grows by [`CHILD_REBOOT_STEP`] at each of its
    /// reboots.
    fn session(&self, route: DeviceRoute, world: &World) -> u32 {
        self.session + CHILD_REBOOT_STEP * Self::reboots(route, world)
    }

    /// The hash of the RPC table at `route` now. It changes with each boot,
    /// and at the hub child with each of its reboots, as a device's table
    /// changes only with new firmware, which comes with a new session. So a
    /// table read before either never matches the device after it.
    fn table_hash(&self, route: DeviceRoute, world: &World) -> u32 {
        TABLE_HASH + self.index + CHILD_REBOOT_STEP * Self::reboots(route, world)
    }
}

/// Serves the device on [`DEVICE_PORT`] until its host stops. Like the
/// firmware, it serves one connection at a time. It listens only while it
/// serves none, so a connect that comes while a connection lives is refused.
pub(super) async fn device(world: Arc<World>) -> ::turmoil::Result {
    let index = world.boots.fetch_add(1, Ordering::SeqCst);
    let boot = Arc::new(Boot {
        index,
        session: 7 + index,
        born: Instant::now(),
        unsent: AtomicU32::new(0),
    });
    world.write(faults::Event::Boot {
        index,
        session: boot.session,
    });
    loop {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, DEVICE_PORT)).await?;
        let (stream, _) = listener.accept().await?;
        drop(listener);
        let connection = world.accepted.fetch_add(1, Ordering::SeqCst);
        world.write(faults::Event::Accepted);
        serve(stream, boot.clone(), connection, world.clone()).await;
    }
}

/// How long what the device wrote may go unread by its peer before the
/// device drops the connection. The firmware drops a peer that has not
/// acknowledged what it sent for 20 s. turmoil has no acknowledgements, and
/// gives a write's credit back only once the peer reads the segment, so the
/// device counts the 20 s from the last moment its peer read while some of
/// what it wrote was unread. It therefore also drops a peer that
/// acknowledges and stops reading, which the firmware never does. The proxy
/// reads its connection to the device for as long as it holds it, so no
/// case reaches that. The firmware's keepalive of 5 s probes only an idle
/// connection, and the device sends a heartbeat every 300 ms.
const UNACKNOWLEDGED: Duration = Duration::from_secs(20);

/// What the peer of a connection of the device has read of what the device
/// wrote, which the device takes as acknowledged.
struct Acknowledged {
    local: SocketAddr,
    peer: SocketAddr,
    /// How many bytes the device has written.
    written: u64,
    /// How many of them the peer had read when the device last looked.
    read: u64,
    /// When the peer last read, or the device last wrote with nothing
    /// unread.
    since: Instant,
}

impl Acknowledged {
    fn of(local: SocketAddr, peer: SocketAddr) -> Acknowledged {
        Acknowledged {
            local,
            peer,
            written: 0,
            read: 0,
            since: Instant::now(),
        }
    }

    /// Takes in what the peer has read since the device last looked.
    fn look(&mut self) {
        let read = crate::seam::turmoil::bytes_read(self.peer, self.local);
        if read != self.read {
            self.read = read;
            self.since = Instant::now();
        }
    }

    /// Counts `bytes` the device is about to write.
    fn write(&mut self, bytes: usize) {
        self.look();
        if self.read >= self.written {
            self.since = Instant::now();
        }
        self.written += bytes as u64;
    }

    /// When the device drops its peer, unless the peer reads before then,
    /// or never while nothing is unread.
    fn deadline(&mut self) -> Instant {
        self.look();
        match self.written > self.read {
            true => self.since + UNACKNOWLEDGED,
            false => Instant::now() + UNACKNOWLEDGED,
        }
    }

    /// Whether the peer has left what the device wrote unread for
    /// [`UNACKNOWLEDGED`].
    fn given_up(&mut self) -> bool {
        self.look();
        self.written > self.read && self.since + UNACKNOWLEDGED <= Instant::now()
    }
}

/// The device's `dev.metadata` reply in `session`.
fn metadata_reply(session: u32) -> Vec<u8> {
    let mut reply = vec![0u8; crate::proto::data::MAX_METADATA_REPLY_SIZE];
    let records = metadata_records(session);
    let written = records.iter().fold(0, |written, record| {
        let frame = record.write_reply_frame(&mut reply[written..]);
        written + frame.expect("a record that fits")
    });
    reply.truncate(written);
    reply
}

/// Answers the RPCs that arrive on `reading`, the device's connection
/// `connection` in `boot`, through `replies`, until the connection ends.
/// `test.echo` replies with the boot's session as four little endian bytes,
/// then its arguments.
async fn answer_calls(
    mut reading: impl AsyncReadExt + Unpin,
    replies: tokio::sync::mpsc::UnboundedSender<Packet>,
    boot: &Boot,
    connection: u32,
    world: &World,
) {
    let mut received = Vec::new();
    let mut chunk = [0; 512];
    loop {
        let n = match reading.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        received.extend_from_slice(&chunk[..n]);
        while let Ok((packet, used)) = Packet::from_slice_prefix(&received) {
            received.drain(..used);
            let Payload::RpcRequest(request) = packet.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            let route = packet.route();
            crate::chan::lock(&world.calls).push((route, name.to_vec()));
            let withheld = world.withholds_metadata.load(Ordering::SeqCst);
            let hash = boot.table_hash(route, world);
            let value = match name {
                b"dev.session" => Some(boot.session(route, world).to_le_bytes().to_vec()),
                b"dev.metadata" if withheld => None,
                b"dev.metadata" => Some(metadata_reply(boot.session)),
                b"test.echo" => Some([&boot.session.to_le_bytes()[..], request.args].concat()),
                _ => answer(name, request.args, hash),
            };
            world.write(faults::Event::Asked {
                index: boot.index,
                connection,
                child: route != DeviceRoute::root(),
                name: String::from_utf8_lossy(name).into_owned(),
                args: request.args.to_vec(),
                hash: (name == b"rpc.hash").then_some(hash),
            });
            if let Some(value) = value {
                let reply = Packet::rpc_reply(request.id.value(), &value, route);
                let _ = replies.send(reply.expect("a reply that fits"));
            }
        }
    }
}

/// Serves `connection` of `boot`. It gets the device's records, unless the
/// device withholds them, then a heartbeat of the root and of the hub child,
/// each with its session, every 300 ms, and a sample each second. Its first
/// sample number is the larger of the whole seconds since the boot and one
/// more than the highest the boot has sent, and it counts on its own from
/// there. Each sample's one column holds the session. The connection ends
/// at the end of what it reads, at a write that fails, or once its peer has
/// left what it wrote unread for [`UNACKNOWLEDGED`].
async fn serve(stream: TcpStream, boot: Arc<Boot>, connection: u32, world: Arc<World>) {
    let root = DeviceRoute::root();
    let session = boot.session;
    let (Ok(local), Ok(peer)) = (stream.local_addr(), stream.peer_addr()) else {
        return world.write(faults::Event::Ended { connection });
    };
    let mut acknowledged = Acknowledged::of(local, peer);
    let (reading, mut writing) = stream.into_split();
    let (replies, mut queued) = tokio::sync::mpsc::unbounded_channel();
    let (read_end, mut ended) = crate::chan::oneshot::channel::<()>();
    let answering = crate::chan::spawn({
        let (boot, world) = (boot.clone(), world.clone());
        async move {
            answer_calls(reading, replies, &boot, connection, &world).await;
            let _ = read_end.send(());
        }
    });
    let end = || world.write(faults::Event::Ended { connection });
    let records = match world.withholds_metadata.load(Ordering::SeqCst) {
        true => Vec::new(),
        false => description(root, session),
    };
    let mut beat = tokio::time::interval(Duration::from_millis(300));
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut next = (boot.born.elapsed().as_secs() as u32).max(boot.unsent.load(Ordering::SeqCst));
    let mut outgoing = records;
    loop {
        for packet in outgoing.drain(..) {
            acknowledged.write(packet.as_bytes().len());
            loop {
                // A write that waits for credit writes nothing, so one
                // dropped at the deadline is made again in full.
                let deadline = acknowledged.deadline();
                tokio::select! {
                    written = writing.write_all(packet.as_bytes()) => match written {
                        Ok(()) => break,
                        Err(_) => {
                            end();
                            return answering.abort();
                        }
                    },
                    _ = sleep_until(deadline) => if acknowledged.given_up() {
                        end();
                        return answering.abort();
                    },
                    _ = &mut ended => return end(),
                }
            }
        }
        let deadline = acknowledged.deadline();
        tokio::select! {
            _ = &mut ended => return end(),
            _ = sleep_until(deadline) => if acknowledged.given_up() {
                end();
                return answering.abort();
            },
            _ = beat.tick() => {
                let child_beats = world.has_child.load(Ordering::SeqCst)
                    && !world.child_silent.load(Ordering::SeqCst);
                if child_beats {
                    outgoing.push(Packet::heartbeat_session(boot.session(child(), &world), child()));
                }
                outgoing.push(Packet::heartbeat_session(session, root));
            }
            Some(reply) = queued.recv() => outgoing.push(reply),
            _ = tick.tick(), if boot.born.elapsed().as_secs() as u32 >= next => {
                let column = (session as f32).to_le_bytes();
                let sample = Packet::samples(1, 0, next, &column, root);
                outgoing.push(sample.expect("a sample that fits"));
                boot.unsent.fetch_max(next + 1, Ordering::SeqCst);
                next += 1;
            }
        }
    }
}

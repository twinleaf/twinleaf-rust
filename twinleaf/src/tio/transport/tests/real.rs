use super::fake::{FakeStream, PIPE_SIZE};
use crate::chan::{Receiver, RecvTimeoutError};
#[cfg(feature = "serial")]
use bytes::BytesMut;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use crate::rt;
use crate::tio::transport::*;

/// A TCP URL on this machine is local by address or by `localhost`. Any
/// other scheme or host is not.
#[test]
fn only_a_tcp_url_on_this_machine_is_local_tcp() {
    for url in [
        "tcp://127.0.0.1:7855",
        "tcp://localhost",
        "tcp4://localhost:17855",
        "tcp6://[::1]:7855",
        "tcp6://::1",
    ] {
        assert!(is_local_tcp(url), "{url}");
    }
    for url in [
        "udp://127.0.0.1:7855",
        "udp4://localhost",
        "tcp://192.0.2.1:7855",
        "tcp://sensor.local",
        "127.0.0.1:7855",
        "serial:///dev/ttyACM0",
        "auto",
    ] {
        assert!(!is_local_tcp(url), "{url}");
    }
}

/// A `.local` name is found in any case, with or without its trailing
/// dot, and with or without a port. Nothing else is a `.local` name.
#[test]
fn a_local_name_is_found_in_any_case_with_or_without_its_dot() {
    let found = |host: &str, port| Some((host.to_string(), port));
    assert_eq!(local_name("Sensor.LOCAL"), found("sensor.local", 7855));
    assert_eq!(
        local_name("sensor.local.:7000"),
        found("sensor.local", 7000)
    );
    assert_eq!(local_name("sensor.local:7000"), found("sensor.local", 7000));
    assert_eq!(local_name("sensor.local."), found("sensor.local", 7855));

    for other in [
        ".local",
        "local",
        "sensor.local:port",
        "sensor.example.com:7855",
        "192.168.1.2:7855",
        "[fe80::1]:7855",
        "fe80::1",
    ] {
        assert_eq!(local_name(other), None, "{other}");
    }
}

/// Of the addresses mDNS finds, the one taken is of the version the URL
/// allows, routable before link-local and IPv4 before IPv6. A link-local
/// IPv6 address keeps its scope.
#[test]
fn an_mdns_address_is_chosen_by_version_and_reach() {
    let addrs = |list: &[&str]| -> Vec<SocketAddr> {
        list.iter().map(|addr| addr.parse().unwrap()).collect()
    };
    let all = addrs(&[
        "127.0.0.1:7855",
        "169.254.3.4:7855",
        "[fe80::1%3]:7855",
        "[fd00::5]:7855",
        "192.168.1.20:7855",
        "10.0.0.9:7855",
        "0.0.0.0:7855",
    ]);
    let chosen = |found: &[SocketAddr], family| preferred(found, &family);

    assert_eq!(
        chosen(&all, AddrFamilyRestrict::Either),
        Some("10.0.0.9:7855".parse().unwrap())
    );
    assert_eq!(
        chosen(&all, AddrFamilyRestrict::V4),
        Some("10.0.0.9:7855".parse().unwrap())
    );
    assert_eq!(
        chosen(&all, AddrFamilyRestrict::V6),
        Some("[fd00::5]:7855".parse().unwrap())
    );

    let local = addrs(&["[fe80::1%3]:7855", "169.254.3.4:7855", "127.0.0.1:7855"]);
    assert_eq!(
        chosen(&local, AddrFamilyRestrict::Either),
        Some("169.254.3.4:7855".parse().unwrap())
    );
    let Some(SocketAddr::V6(scoped)) = chosen(&local, AddrFamilyRestrict::V6) else {
        panic!("no IPv6 address was chosen");
    };
    assert_eq!((scoped.ip().segments()[0], scoped.scope_id()), (0xfe80, 3));

    let v4_only = addrs(&["192.168.1.20:7855", "0.0.0.0:7855"]);
    assert_eq!(chosen(&v4_only, AddrFamilyRestrict::V6), None);
    assert_eq!(chosen(&[], AddrFamilyRestrict::Either), None);
}

/// A loopback connect waits for a handshake as long as any other, but a
/// port nothing listens on refuses it at once.
#[test]
fn a_refused_loopback_connect_fails_at_once() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let started = Instant::now();
    let refused = rt::handle().block_on(tcp::connect(&[address])).map(drop);

    let took = started.elapsed();
    assert_eq!(
        refused.map_err(|error| error.kind()),
        Err(io::ErrorKind::ConnectionRefused)
    );
    assert!(took < Duration::from_secs(1), "it took {took:?}");
}

#[test]
fn dropping_idle_port_closes_transport() {
    let (port, mut peer, results) = tcp_port_reporting();
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    // Confirm the task is receiving, with nothing queued to send.
    peer.write_all(Packet::heartbeat(proto::DeviceRoute::root()).as_bytes())
        .unwrap();
    assert!(results
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    drop(port);

    // The peer stays connected and sends nothing else to wake the task.
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}

/// Backpressure delays packets and never eats them. Every packet a caller
/// queued reaches the peer once the peer starts reading again.
#[test]
fn a_peer_that_stops_reading_loses_no_queued_packet() {
    let (port, mut peer, _results) = tcp_port_reporting();

    let packet = Packet::rpc_reply(0, &[0u8; 498], proto::DeviceRoute::root()).unwrap();
    let queued = 4000;
    for _ in 0..queued {
        port.try_send(packet.clone()).unwrap();
    }
    drop(port);

    let mut sunk = Vec::new();
    peer.read_to_end(&mut sunk).unwrap();
    assert_eq!(sunk.len(), queued * packet.as_bytes().len());
}

/// A port over a [`FakeStream`], and what a test watches it with.
struct Fake {
    port: Port,
    /// What the port receives.
    results: Receiver<ReceiveResult>,
    /// Disconnects once the task has dropped the stream.
    gone: mpsc::Receiver<()>,
    /// The other end of the pipe.
    peer: DuplexStream,
}

/// Returns a port over a [`FakeStream`] whose task runs on the test's
/// runtime, follows `rules` and frames packets with `framing`. A `stuck`
/// stream starts with its pipe full, so it writes nothing until the peer
/// reads.
fn fake<C>(rules: Rules, framing: C, stuck: bool) -> Fake
where
    C: tokio_util::codec::Decoder<Item = TransportEvent, Error = RecvError>
        + tokio_util::codec::Encoder<Packet, Error = RecvError>
        + Send
        + Unpin
        + 'static,
{
    let (pipe, peer) = tokio::io::duplex(PIPE_SIZE);
    let (mut stream, gone, _) = FakeStream::new(pipe);
    let (rx, results) = crate::chan::bounded(DEFAULT_RX_CHANNEL_SIZE);
    let (port, task) =
        Port::with_task(TransportKind::Tcp, None, rules, rx, DEFAULT_TX_CHANNEL_SIZE);
    let run = async move {
        if stuck {
            stream.write_all(&[0; PIPE_SIZE]).await.unwrap();
        }
        task.run_stream(stream, framing).await;
    };
    Fake {
        port: port.start(&Supervisor::current(), "a test port", run),
        results,
        gone,
        peer,
    }
}

/// A peer that never takes another byte cannot pin the port's task,
/// whoever still holds the port. The task lets go of the link once its
/// write budget is spent, and not before.
#[tokio::test(start_paused = true)]
async fn a_port_stuck_draining_gives_up_on_its_write_budget() {
    let Fake {
        port,
        gone,
        peer: _peer,
        ..
    } = fake(Rules::NONE, codec::tcp::Codec::default(), true);

    port.try_send(Packet::heartbeat(proto::DeviceRoute::root()))
        .unwrap();

    tokio::time::sleep(DRAIN_TIMEOUT - Duration::from_millis(1)).await;
    assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(matches!(
        gone.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
}

/// A link that closes in stages shuts down its sending side once the
/// port is dropped and its queue is written. The peer reads all of it,
/// then the end of the stream, and the task lets go once the peer closes.
#[tokio::test(start_paused = true)]
async fn a_link_closing_in_stages_lets_go_once_its_peer_closes() {
    let Fake {
        port,
        gone,
        mut peer,
        ..
    } = fake(tcp::CLIENT_RULES, codec::tcp::Codec::default(), false);
    let last = Packet::heartbeat(proto::DeviceRoute::root());
    port.try_send(last.clone()).unwrap();
    drop(port);

    let mut read = Vec::new();
    peer.read_to_end(&mut read).await.unwrap();
    assert_eq!(read, last.as_bytes());
    assert!(
        matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "the task let go before its peer closed"
    );

    drop(peer);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(matches!(
        gone.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
}

/// A peer that never closes holds a link closing in stages for the time
/// the link waits, and no longer, though it keeps sending.
#[tokio::test(start_paused = true)]
async fn a_link_closing_in_stages_lets_go_of_a_peer_that_never_closes() {
    let Fake {
        port,
        gone,
        mut peer,
        ..
    } = fake(tcp::CLIENT_RULES, codec::tcp::Codec::default(), false);
    drop(port);
    let mut read = Vec::new();
    peer.read_to_end(&mut read).await.unwrap();

    let beat = Packet::heartbeat(proto::DeviceRoute::root());
    let sending = crate::chan::spawn(async move {
        while peer.write_all(beat.as_bytes()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    tokio::time::sleep(tcp::CLIENT_HALF_CLOSE - Duration::from_millis(1)).await;
    assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(matches!(
        gone.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    sending.await.unwrap();
}

/// Closing a port returns once its task has dropped the link. A task
/// stuck writing is ended once the wait runs out, which drops the link
/// before the close returns.
#[tokio::test(start_paused = true)]
async fn closing_a_port_waits_for_its_task_and_then_ends_it() {
    let idle = fake(Rules::NONE, codec::tcp::Codec::default(), false);
    assert!(idle.port.close(Duration::from_secs(2)).await);
    assert!(matches!(
        idle.gone.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));

    let stuck = fake(Rules::NONE, codec::tcp::Codec::default(), true);
    stuck
        .port
        .try_send(Packet::heartbeat(proto::DeviceRoute::root()))
        .unwrap();
    let closing = tokio::time::Instant::now();
    assert!(!stuck.port.close(Duration::from_millis(200)).await);
    assert_eq!(closing.elapsed(), Duration::from_millis(200));
    assert!(matches!(
        stuck.gone.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
}

/// A kept alive port sends its beat while idle, stays open while its peer
/// sends, and gives the peer up once it has been quiet too long.
#[tokio::test(start_paused = true)]
async fn a_kept_alive_port_beats_and_gives_up_a_quiet_peer() {
    let (send, receive) = crate::chan::unbounded();
    let (port, mut peer) = Port::fake(TransportKind::Tcp, None, send);
    let keep_alive = KeepAlive {
        beat: Packet::alive,
        every: Duration::from_secs(1),
        quiet: Duration::from_secs(5),
    };
    port.keep_alive(keep_alive).await;
    let kept = tokio::time::Instant::now();

    for _ in 0..8 {
        let beat = peer.recv().await.expect("the port beats");
        assert!(matches!(
            beat.payload(),
            packet::Payload::ProxyRx(packet::ProxyRx::Alive)
        ));
        peer.send(&Packet::heartbeat(proto::DeviceRoute::root()))
            .await;
    }
    assert_eq!(kept.elapsed(), Duration::from_secs(8));
    let quiet = tokio::time::Instant::now();
    while let Ok(Ok(_)) = receive.recv_async().await {}

    assert_eq!(quiet.elapsed(), keep_alive.quiet);
    assert!(peer.released());
}

/// A terminal read error is reported once and releases the link even
/// while the caller retains its port handle.
#[tokio::test]
async fn a_read_error_ends_the_transport_without_dropping_the_port() {
    let (send, receive) = crate::chan::unbounded();
    let (port, mut peer) = Port::fake(TransportKind::Tcp, None, send);
    peer.fail(io::ErrorKind::ConnectionReset);
    assert!(matches!(
        receive.recv_async().await.unwrap(),
        Err(RecvError::IO(error)) if error.kind() == io::ErrorKind::ConnectionReset
    ));
    assert!(matches!(
        receive.recv_async().await,
        Err(crate::chan::RecvError::Disconnected)
    ));
    assert!(peer.released());
    assert!(matches!(
        port.try_send(Packet::heartbeat(proto::DeviceRoute::root())),
        Err(SendError::Disconnected)
    ));
}

/// A stalled sink does not keep incoming packets from the framed source.
#[tokio::test(start_paused = true)]
async fn a_blocked_framed_write_keeps_receiving() {
    let Fake {
        port,
        results,
        gone,
        mut peer,
    } = fake(Rules::NONE, codec::tcp::Codec::default(), true);
    let packet = Packet::heartbeat_session(7, proto::DeviceRoute::root());
    port.try_send(packet.clone()).unwrap();
    peer.write_all(packet.as_bytes()).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), results.recv_async())
        .await
        .expect("a pending write must not block reads")
        .unwrap()
        .unwrap();
    let TransportEvent::Packet(received) = received else {
        panic!("expected the incoming heartbeat")
    };
    assert_eq!(received.as_bytes(), packet.as_bytes());
    assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
}

/// A UDP link to `localhost` reaches a device listening on IPv4 only,
/// although the name may resolve to `::1` first, as it does on Windows.
#[tokio::test]
async fn a_udp_link_to_localhost_reaches_an_ipv4_device() {
    let device = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://localhost:{}", device.local_addr().unwrap().port());
    let (send, _received) = crate::chan::unbounded();
    let _port = Port::open(&url, send, &Supervisor::current())
        .await
        .unwrap();

    let mut datagram = [0; 512];
    let heartbeat = device.recv_from(&mut datagram);
    tokio::time::timeout(Duration::from_secs(2), heartbeat)
        .await
        .expect("the link's heartbeat reaches the device")
        .unwrap();
}

/// Datagram framing rejects malformed and empty datagrams independently,
/// and sends each outgoing packet as exactly one datagram.
#[tokio::test]
async fn udp_framing_recovers_at_the_next_datagram() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let (send, received) = crate::chan::unbounded();
    let (port, task) = Port::with_task(
        TransportKind::Udp,
        None,
        Rules::NONE,
        send,
        DEFAULT_TX_CHANNEL_SIZE,
    );
    let port = port.start(
        &Supervisor::current(),
        "a UDP framing test",
        task.run_datagram(socket, peer.local_addr().unwrap()),
    );
    let packet = Packet::heartbeat_session(7, proto::DeviceRoute::root());
    let mut trailing = packet.as_bytes().to_vec();
    trailing.push(0);
    let stranger = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    stranger
        .send_to(
            Packet::heartbeat_session(999, proto::DeviceRoute::root()).as_bytes(),
            local,
        )
        .await
        .unwrap();
    peer.send_to(&trailing, local).await.unwrap();
    peer.send_to(&[], local).await.unwrap();
    peer.send_to(packet.as_bytes(), local).await.unwrap();
    let arrivals = async {
        assert!(matches!(
            received.recv_async().await.unwrap(),
            Ok(TransportEvent::RejectedFrame(
                packet::DecodeError::PacketTooBig
            ))
        ));
        assert!(matches!(
            received.recv_async().await.unwrap(),
            Ok(TransportEvent::RejectedFrame(
                packet::DecodeError::PacketTooSmall
            ))
        ));
        let Ok(TransportEvent::Packet(actual)) = received.recv_async().await.unwrap() else {
            panic!("the following datagram did not decode")
        };
        assert_eq!(actual.as_bytes(), packet.as_bytes());
    };
    tokio::time::timeout(Duration::from_secs(2), arrivals)
        .await
        .unwrap();
    port.try_send(packet.clone()).unwrap();
    let mut raw = [0; 1024];
    let size = tokio::time::timeout(Duration::from_secs(2), peer.recv(&mut raw))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "UDP send did not arrive, transport reported {:?}",
                received.try_recv()
            )
        })
        .unwrap();
    assert_eq!(&raw[..size], packet.as_bytes());
}

/// Both checksum failures and valid-checksum frames with trailing bytes
/// are consumed as rejected frames, and leave the next packet readable.
#[cfg(feature = "serial")]
#[tokio::test]
async fn rejected_serial_frames_leave_the_transport_open() {
    use codec::serial::Codec;
    let Fake {
        port: _port,
        results,
        gone,
        mut peer,
    } = fake(Rules::NONE, Codec::default(), false);
    let packet = Packet::heartbeat(proto::DeviceRoute::root());
    let mut frame = BytesMut::new();
    Codec::default().encode(&packet, &mut frame);
    let mut bad_crc = frame.to_vec();
    bad_crc[2] ^= 1;
    peer.write_all(&bad_crc).await.unwrap();

    let mut trailing = packet.as_bytes().to_vec();
    trailing.push(0);
    let mut encoded = vec![0; proto::serial::max_serialized_size(trailing.len())];
    let size = proto::serial::serialize(&trailing, &mut encoded).unwrap();
    peer.write_all(&encoded[..size]).await.unwrap();
    peer.write_all(&frame).await.unwrap();

    assert!(matches!(
        results.recv_async().await.unwrap(),
        Ok(TransportEvent::RejectedFrame(packet::DecodeError::CRC32))
    ));
    assert!(matches!(
        results.recv_async().await.unwrap(),
        Ok(TransportEvent::RejectedFrame(
            packet::DecodeError::PacketTooBig
        ))
    ));
    let Ok(TransportEvent::Packet(next)) = results.recv_async().await.unwrap() else {
        panic!("the valid packet after the rejected frames did not arrive")
    };
    assert_eq!(next.as_bytes(), packet.as_bytes());
    assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
}

/// Reads one packet of `size` bytes from `peer`.
async fn read_packet(peer: &mut DuplexStream, size: usize) -> Vec<u8> {
    let mut packet = vec![0; size];
    peer.read_exact(&mut packet).await.unwrap();
    packet
}

/// A port with a heartbeat sends one once it has written nothing for
/// the interval. Any packet it writes puts the next heartbeat off.
#[tokio::test(start_paused = true)]
async fn a_quiet_port_sends_a_heartbeat_each_interval() {
    let interval = Duration::from_millis(100);
    let rules = Rules {
        heartbeat: Some(interval),
        ..Rules::NONE
    };
    let Fake { port, mut peer, .. } = fake(rules, codec::tcp::Codec::default(), false);
    let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());
    let request = Packet::rpc_request("dev.name", &[], 7, proto::DeviceRoute::root()).unwrap();
    let opened = tokio::time::Instant::now();
    let at = |offset: u64| {
        let elapsed = opened.elapsed().as_millis();
        assert!(
            (offset as u128..=offset as u128 + 1).contains(&elapsed),
            "arrived at {elapsed} ms, not {offset} ms"
        );
    };

    let first = read_packet(&mut peer, heartbeat.as_bytes().len()).await;
    assert_eq!(first, heartbeat.as_bytes());
    at(100);

    tokio::time::sleep(Duration::from_millis(50)).await;
    port.try_send(request.clone()).unwrap();
    assert_eq!(
        read_packet(&mut peer, request.as_bytes().len()).await,
        request.as_bytes()
    );
    at(150);

    read_packet(&mut peer, heartbeat.as_bytes().len()).await;
    at(250);
}

/// During its holdoff a serial port discards the first bytes to arrive,
/// even text, and every packet after them, but passes text on. It sends
/// heartbeats and holds what is queued until the holdoff ends.
#[cfg(feature = "serial")]
#[tokio::test(start_paused = true)]
async fn a_serial_port_in_its_holdoff_passes_only_text() {
    use codec::serial::{Codec, Frame};

    let Fake {
        port,
        results,
        mut peer,
        ..
    } = fake(
        serial::RULES,
        Codec::with_timing(serial::RULES.holdoff, serial::RULES.stale_after),
        false,
    );
    let opened = tokio::time::Instant::now();
    let frame = |route: &str| {
        let packet = Packet::heartbeat(route.parse().unwrap());
        let mut frame = BytesMut::new();
        Codec::default().encode(&packet, &mut frame);
        frame
    };
    let step = Duration::from_millis(10);

    port.try_send(Packet::heartbeat("/7".parse().unwrap()))
        .unwrap();
    peer.write_all(b"stale\n").await.unwrap();
    tokio::time::sleep(step).await;
    peer.write_all(b"boot\n").await.unwrap();
    peer.write_all(&frame("/2")).await.unwrap();

    // The peer sees heartbeats to the root, then the queued packet once
    // the holdoff is over.
    let mut decoder = Codec::default();
    let mut sent = BytesMut::new();
    let first_route = loop {
        peer.read_buf(&mut sent).await.unwrap();
        if let Some(Frame::Packet(packet)) = decoder.decode(&mut sent).unwrap() {
            if packet.route() != proto::DeviceRoute::root() {
                break packet.route();
            }
        }
    };
    assert_eq!(first_route, "/7".parse().unwrap());
    assert!(opened.elapsed() >= serial::RULES.holdoff.unwrap());

    peer.write_all(&frame("/3")).await.unwrap();
    tokio::time::sleep(step).await;
    assert!(matches!(results.try_recv(), Ok(Ok(TransportEvent::Text(text))) if text == "boot"));
    let TransportEvent::Packet(after) = results.try_recv().unwrap().unwrap() else {
        panic!("expected a packet")
    };
    assert_eq!(after.route(), "/3".parse().unwrap());
    assert!(results.try_recv().is_err());
}

/// A task that sends to a full queue waits for room, and every packet
/// still reaches the peer in order.
#[test]
fn sending_to_a_full_queue_waits_for_room() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let (rx, _results) = crate::chan::bounded(DEFAULT_RX_CHANNEL_SIZE);
    let port = Port::from_tcp_stream(stream, rx, 1).unwrap();

    let packets: Vec<Packet> = (0..2000)
        .map(|id| Packet::rpc_reply(id, &[0u8; 498], proto::DeviceRoute::root()).unwrap())
        .collect();
    let expected: Vec<u8> = packets.iter().flat_map(|p| p.as_bytes()).copied().collect();
    let reader = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        let mut sunk = Vec::new();
        peer.read_to_end(&mut sunk).unwrap();
        sunk
    });
    crate::chan::spawn_on(rt::handle(), async move {
        for packet in packets {
            port.send_async(packet).await.unwrap();
        }
    });

    assert!(reader.join().unwrap() == expected);
}

/// A TCP port, its peer, and the channel it delivers what it receives
/// to.
fn tcp_port_reporting() -> (Port, std::net::TcpStream, Receiver<ReceiveResult>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("tcp://{}", listener.local_addr().unwrap());
    let (received, results) = crate::chan::bounded(DEFAULT_RX_CHANNEL_SIZE);
    let port = Port::new_on(&url, received, &Supervisor::new()).unwrap();
    let (peer, _) = listener.accept().unwrap();
    (port, peer, results)
}

/// An address that names no port gets the default one, in brackets for
/// an IPv6 address. One that names a port keeps it.
#[test]
fn an_address_without_a_port_gets_the_default() {
    assert_eq!(with_port("10.0.0.2"), "10.0.0.2:7855");
    assert_eq!(with_port("10.0.0.2:99"), "10.0.0.2:99");
    assert_eq!(with_port("::1"), "[::1]:7855");
    assert_eq!(with_port("[::1]"), "[::1]:7855");
    assert_eq!(with_port("[::1]:99"), "[::1]:99");
    assert_eq!(with_port("sensor.example"), "sensor.example:7855");
    assert_eq!(with_port("sensor.example:99"), "sensor.example:99");
}

/// A name resolves to every address of the IP version its scheme allows,
/// and a connect goes on to the next address when one refuses.
#[test]
fn a_connect_tries_each_address_the_name_resolves_to() {
    let resolved = rt::handle().block_on(resolve(
        "localhost:99",
        AddrFamilyRestrict::V4,
        &Supervisor::new(),
    ));
    assert!(resolved
        .unwrap()
        .iter()
        .all(|found| found.is_ipv4() && found.port() == 99));
    let ip_v6 = rt::handle().block_on(resolve(
        "127.0.0.1",
        AddrFamilyRestrict::V6,
        &Supervisor::new(),
    ));
    assert_eq!(
        ip_v6.map_err(|error| error.kind()),
        Err(io::ErrorKind::NotFound)
    );

    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let refusing = closed.local_addr().unwrap();
    drop(closed);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let answering = listener.local_addr().unwrap();
    let connected = rt::handle().block_on(tcp::connect(&[refusing, answering]));
    assert_eq!(connected.unwrap().peer_addr().unwrap(), answering);
}

/// A TCP port opened as a future connects without a blocking thread, and
/// delivers what its peer sends.
#[test]
fn a_tcp_port_opened_as_a_future_delivers_what_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("tcp://{}", listener.local_addr().unwrap());
    assert!(Port::opens_as_future(&url));
    let (received, results) = crate::chan::bounded(DEFAULT_RX_CHANNEL_SIZE);
    let tasks = Supervisor::new();
    let opening = tasks.run("a test open", {
        let tasks = tasks.clone();
        async move { Port::open(&url, received, &tasks).await }
    });
    let _port = opening.unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());

    peer.write_all(heartbeat.as_bytes()).unwrap();

    let arrived = results.recv_timeout(Duration::from_secs(2)).unwrap();
    let TransportEvent::Packet(arrived) = arrived.unwrap() else {
        panic!("expected a packet")
    };
    assert_eq!(arrived.as_bytes(), heartbeat.as_bytes());
}

/// A packet whose header parses but whose payload does not is skipped on
/// its own, and the packet after it still arrives.
#[test]
fn a_malformed_tcp_packet_is_skipped_and_the_next_arrives() {
    let (_port, mut peer, results) = tcp_port_reporting();
    let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());

    // A PROXY_RX header announcing an empty payload, which has no kind byte.
    peer.write_all(&[proto::PacketType::PROXY_RX.value(), 0, 0, 0])
        .unwrap();
    peer.write_all(heartbeat.as_bytes()).unwrap();

    let wait = Duration::from_secs(2);
    assert!(matches!(
        results.recv_timeout(wait).unwrap(),
        Ok(TransportEvent::RejectedFrame(
            packet::DecodeError::PayloadTooShort
        ))
    ));
    let TransportEvent::Packet(next) = results.recv_timeout(wait).unwrap().unwrap() else {
        panic!("expected the next packet")
    };
    assert_eq!(next.as_bytes(), heartbeat.as_bytes());
}

/// A header that does not parse leaves no way to find the next packet, so
/// the error is reported once and the port closes.
#[test]
fn a_tcp_stream_that_loses_its_framing_closes() {
    let (_port, mut peer, results) = tcp_port_reporting();
    let wait = Duration::from_secs(2);
    peer.set_read_timeout(Some(wait)).unwrap();

    let mut wire = vec![0, 0, 0, 0];
    wire.extend_from_slice(Packet::heartbeat(proto::DeviceRoute::root()).as_bytes());
    peer.write_all(&wire).unwrap();
    assert!(matches!(
        results.recv_timeout(wait).unwrap(),
        Err(RecvError::LostFraming(
            packet::DecodeError::InvalidPacketType
        ))
    ));
    assert!(matches!(
        results.recv_timeout(wait),
        Err(RecvTimeoutError::Disconnected)
    ));
    // The port may close with the heartbeat still unread, which resets
    // the connection rather than ending it. Either way it is closed.
    match peer.read(&mut [0]) {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
        other => panic!("the stream did not close: {other:?}"),
    }
}

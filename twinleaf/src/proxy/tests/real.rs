use super::fixtures::{options, FakeDevice, WAIT};
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, PacketType};
use crate::proxy::*;
use crate::rt::{self, Supervisor};
use crate::session::{decode, encode, Introduction};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::transport;
use std::net::{Ipv4Addr, Ipv6Addr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// The `proxy.hello` of a client named `caller` with pid 4242.
fn caller_hello() -> Vec<u8> {
    encode(&Introduction {
        pid: 4242,
        ..Introduction::of_this_process("caller")
    })
}

/// The introduction a proxy hosted by this process under `name` replies
/// to `proxy.hello` with.
fn hosted_by(name: &str) -> Introduction {
    Introduction::of_this_process(name)
}

/// Decodes the reply to `proxy.hello`.
fn host_of(reply: io::Result<Vec<u8>>) -> Introduction {
    decode::<Introduction>(&reply.unwrap()).unwrap()
}

/// A client speaking raw packets to a proxy.
struct TestClient {
    port: transport::Port,
    rx: crate::chan::Receiver<transport::ReceiveResult>,
}

impl TestClient {
    fn connect(endpoint: &str) -> TestClient {
        let (sender, rx) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let port = transport::Port::new_on(endpoint, sender, &Supervisor::new());
        TestClient {
            port: port.unwrap(),
            rx,
        }
    }

    /// A client over `stream`, which a test may also write to as it is.
    fn over(stream: TcpStream) -> TestClient {
        let (sender, rx) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let port =
            transport::Port::from_tcp_stream(stream, sender, transport::DEFAULT_TX_CHANNEL_SIZE);
        TestClient {
            port: port.unwrap(),
            rx,
        }
    }

    fn send(&self, pkt: Packet) {
        self.port.try_send(pkt).unwrap();
    }

    /// The first packet `pick` accepts, skipping any other.
    fn expect<T>(&self, mut pick: impl FnMut(&Packet) -> Option<T>) -> T {
        let deadline = Instant::now() + WAIT;
        loop {
            match self.rx.recv_deadline(deadline) {
                Ok(Ok(transport::TransportEvent::Packet(pkt))) => {
                    if let Some(found) = pick(&pkt) {
                        return found;
                    }
                }
                Ok(Ok(event)) => panic!("unexpected transport event: {event:?}"),
                Ok(Err(error)) => panic!("the client could not receive: {error}"),
                Err(crate::chan::RecvTimeoutError::Timeout) => {
                    panic!("nothing expected arrived within {WAIT:?}")
                }
                Err(crate::chan::RecvTimeoutError::Disconnected) => {
                    panic!("the proxy closed the connection")
                }
            }
        }
    }

    /// Wait for the client to hear that the proxy stopped, then for its
    /// connection to close.
    fn expect_stopped(&self) {
        self.expect(|pkt| {
            let Payload::ProxyRx(ProxyRx::Link(status)) = pkt.payload() else {
                return None;
            };
            assert_eq!(status, LinkStatus::Stopped);
            assert_eq!(pkt.route(), DeviceRoute::root());
            Some(())
        });
        self.expect_closed();
    }

    /// Wait for the proxy to close the connection, skipping whatever
    /// arrives first.
    fn expect_closed(&self) {
        let deadline = Instant::now() + WAIT;
        loop {
            match self.rx.recv_deadline(deadline) {
                Ok(_) => {}
                Err(crate::chan::RecvTimeoutError::Disconnected) => return,
                Err(crate::chan::RecvTimeoutError::Timeout) => {
                    panic!("the connection stayed open")
                }
            }
        }
    }

    /// Make a control request and wait for the proxy's answer.
    fn control(&self, method: &str, args: &[u8], id: u16) -> Result<Vec<u8>, (RpcError, String)> {
        self.send(Packet::proxy_request(method, args, id).unwrap());
        self.expect(|pkt| match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Reply(reply)) if reply.req_id.value() == id => {
                Some(Ok(reply.value.to_vec()))
            }
            Payload::ProxyRx(ProxyRx::Error(error)) if error.req_id.value() == id => Some(Err((
                RpcError::from(error.code),
                String::from_utf8_lossy(error.message).into_owned(),
            ))),
            _ => None,
        })
    }

    fn hello(&self, pid: u32, name: &str) {
        let client = Introduction {
            pid,
            ..Introduction::of_this_process(name)
        };
        let reply = self.control("proxy.hello", &encode(&client), 1);
        decode::<Introduction>(&reply.unwrap()).unwrap();
    }

    fn open(&self, target: &str) -> Result<String, (RpcError, String)> {
        self.control("proxy.open", &encode(&target), 2)
            .map(|reply| decode::<String>(&reply).unwrap())
    }

    /// Ask the root device its name.
    fn dev_name(&self, id: u16) -> String {
        self.dev_name_at(DeviceRoute::root(), id)
    }

    /// Ask the device at `route` its name.
    fn dev_name_at(&self, route: DeviceRoute, id: u16) -> String {
        let request = Packet::rpc_request("dev.name", &[], id, route);
        self.send(request.unwrap());
        self.expect(|pkt| match pkt.payload() {
            Payload::RpcReply(reply) if reply.req_id.value() == id => {
                assert_eq!(pkt.route(), route, "the reply came from elsewhere");
                Some(String::from_utf8_lossy(reply.value).into_owned())
            }
            _ => None,
        })
    }
}

/// `TWINLEAF_PROXY` names a proxy, and an empty variable names none.
#[test]
fn the_variable_names_a_proxy_unless_it_is_empty() {
    let url = "tcp://127.0.0.1:2".to_string();
    assert_eq!(named_by(Some(url.clone())), Some(url));
    assert_eq!(named_by(Some(String::new())), None);
    assert_eq!(named_by(None), None);
}

/// A connect the machine stalls past the first try is given longer on a
/// second, which a running proxy then answers.
#[tokio::test(start_paused = true)]
async fn a_connect_that_times_out_once_answers_on_its_second_try() {
    let tries = AtomicUsize::new(0);
    let started = tokio::time::Instant::now();

    let answered = answers_on_retry(|| {
        let stalled = tries.fetch_add(1, Ordering::Relaxed) == 0;
        async move {
            if stalled {
                std::future::pending::<()>().await;
            }
            io::Result::Ok(())
        }
    })
    .await;

    assert!(answered);
    assert_eq!(tries.into_inner(), 2);
    assert_eq!(started.elapsed(), LOOPBACK_CONNECT_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn a_refused_connect_is_not_tried_again() {
    let tries = AtomicUsize::new(0);

    let answered = answers_on_retry(|| {
        tries.fetch_add(1, Ordering::Relaxed);
        async { io::Result::<()>::Err(io::ErrorKind::ConnectionRefused.into()) }
    })
    .await;

    assert!(!answered);
    assert_eq!(tries.into_inner(), 1);
}

#[test]
fn a_proxy_can_be_shared_between_threads() {
    fn shareable<T: Send + Sync>() {}
    shareable::<Proxy>();
}

/// A second proxy on a port in use binds nothing, and says whether a
/// proxy or something else holds the port.
#[test]
fn a_second_proxy_on_a_port_in_use_binds_nothing_and_says_what_holds_it() {
    let first = Proxy::host(options()).unwrap();
    let endpoint = first.endpoint();
    let port = endpoint.rsplit(':').next().unwrap().parse().unwrap();

    let refused = Proxy::host(HostOptions { port, ..options() }).unwrap_err();

    assert_eq!(refused.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(
        refused.to_string(),
        format!(
            "a Twinleaf proxy already runs at {endpoint}, hosted by {}",
            hosted_by("a test")
        )
    );

    let device = FakeDevice::start();
    let port = device.url.rsplit(':').next().unwrap().parse().unwrap();
    let refused = Proxy::host(HostOptions { port, ..options() }).unwrap_err();

    assert_eq!(refused.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(
        refused.to_string(),
        format!(
            "tcp://127.0.0.1:{port} is a proxy from an older build of twinleaf, or no proxy; \
             update tio, or stop it"
        )
    );
}

/// A client whose `localhost` resolves to `::1` first finds the proxy
/// too, until it stops.
#[test]
fn a_proxy_listens_on_both_loopback_addresses() {
    if TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_err() {
        eprintln!("skipped: this host cannot bind ::1");
        return;
    }
    let proxy = Proxy::host(options()).unwrap();
    let endpoint = proxy.endpoint();
    let port: u16 = endpoint.rsplit(':').next().unwrap().parse().unwrap();
    assert_eq!(endpoint, format!("tcp://127.0.0.1:{port}"));
    let hello = caller_hello();

    for endpoint in [endpoint, format!("tcp://[::1]:{port}")] {
        let reply = call(&endpoint, "proxy.hello", &hello, WAIT);
        assert_eq!(host_of(reply), hosted_by("a test"), "{endpoint}");
    }

    proxy.shutdown();
    let addresses: [SocketAddr; 2] = [
        (Ipv4Addr::LOCALHOST, port).into(),
        (Ipv6Addr::LOCALHOST, port).into(),
    ];
    for address in addresses {
        let connected = TcpStream::connect_timeout(&address, WAIT);
        assert!(connected.is_err(), "{address} still answers");
    }
}

/// A client holds a background proxy's default for others to open with
/// `auto`, one holder at a time, until it leaves.
#[test]
fn a_held_default_serves_auto_until_its_holder_leaves() {
    let device = FakeDevice::start();
    let prefix: DeviceRoute = "/3".parse().unwrap();
    let layout = Layout {
        mounts: vec![Mount {
            url: device.url.clone(),
            source: DeviceRoute::root(),
            prefix,
        }],
    };
    let proxy = Proxy::host(HostOptions {
        stoppable: true,
        ..options()
    })
    .unwrap();
    let holder = TestClient::connect(&proxy.endpoint());
    holder.hello(4242, "tio list");
    assert_eq!(
        holder.control("proxy.default", &encode(&layout), 2),
        Ok(Vec::new())
    );

    let client = TestClient::connect(&proxy.endpoint());
    client.hello(4243, "tio monitor");
    assert_eq!(client.open("auto"), Ok("default".to_string()));
    assert_eq!(client.dev_name_at(prefix, 7), "FAKE");
    let rival = TestClient::connect(&proxy.endpoint());
    rival.hello(4244, "rival");
    let refused = rival.control("proxy.default", &encode(&layout), 2);
    assert_eq!(refused.unwrap_err().0, RpcError::State);

    drop(holder);
    let deadline = Instant::now() + Duration::from_secs(5);
    let late = loop {
        let late = TestClient::connect(&proxy.endpoint());
        late.hello(4245, "late");
        match late.open("default") {
            Err((RpcError::Invalid, _)) => break late,
            outcome if Instant::now() >= deadline => {
                panic!("the default outlived its holder: {outcome:?}")
            }
            Ok(_) | Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    assert_eq!(
        late.control("proxy.default", &encode(&layout), 3),
        Ok(Vec::new())
    );
}

/// A proxy a terminal or an app hosts keeps its own default.
#[test]
fn a_hosted_proxy_refuses_to_lend_its_default() {
    let proxy = Proxy::host(options()).unwrap();
    let client = TestClient::connect(&proxy.endpoint());
    client.hello(4242, "tio list");

    let layout = Layout::single("tcp://192.0.2.1");
    let (code, message) = client
        .control("proxy.default", &encode(&layout), 2)
        .unwrap_err();

    assert_eq!(code, RpcError::State);
    assert!(message.contains("a test"), "{message}");
}

/// Returns a proxy whose default is `device`, and a proxy whose default
/// mounts that proxy at `/2`.
fn proxy_behind_proxy(device: &FakeDevice) -> (Proxy, Proxy) {
    let upstream = Proxy::host(HostOptions {
        default: Some(Layout::single(&device.url)),
        ..options()
    })
    .unwrap();
    let mount = Mount {
        url: upstream.endpoint(),
        source: DeviceRoute::root(),
        prefix: "/2".parse().unwrap(),
    };
    let downstream = Proxy::host(HostOptions {
        default: Some(Layout {
            mounts: vec![mount],
        }),
        ..options()
    })
    .unwrap();
    (upstream, downstream)
}

/// A proxy that mounts another proxy says hello to it and opens its
/// `auto`, as a control client does, so the other proxy knows it by name.
/// A client of the first reaches the device behind the second.
#[test]
fn a_proxy_that_mounts_another_is_its_control_client() {
    let device = FakeDevice::start();
    let (upstream, downstream) = proxy_behind_proxy(&device);
    let client = TestClient::connect(&downstream.endpoint());
    client.hello(4242, "two proxies away");
    client.open("default").unwrap();

    assert_eq!(client.dev_name_at("/2".parse().unwrap(), 7), "FAKE");
    let links: Vec<_> = upstream
        .clients()
        .into_iter()
        .filter(|client| client.name.as_deref() == Some("a proxy's link"))
        .collect();
    assert_eq!(links.len(), 1, "{:?}", upstream.clients());
    assert!(links[0].target.is_some());
}

/// A device lost behind a mounted proxy is heard at its mount as a link
/// status, not in the legacy form, and the link between the proxies stays
/// up.
#[test]
fn a_device_lost_behind_a_mounted_proxy_is_a_status_at_its_mount() {
    let device = FakeDevice::start();
    let (upstream, downstream) = proxy_behind_proxy(&device);
    let client = TestClient::connect(&downstream.endpoint());
    client.hello(4242, "two proxies away");
    client.open("default").unwrap();
    assert_eq!(client.dev_name_at("/2".parse().unwrap(), 7), "FAKE");
    let linked = upstream.clients().len();

    device.drop_connections();

    let lost = client.expect(|pkt| match pkt.payload() {
        Payload::ProxyRx(ProxyRx::Link(LinkStatus::SensorDisconnected)) => Some(pkt.route()),
        _ => None,
    });
    assert_eq!(lost, "/2".parse().unwrap());
    assert_eq!(upstream.clients().len(), linked, "the link was dropped");
}

/// A client mounts devices of its own choosing, each where it says, and
/// the table shows what it mounted. A layout whose prefixes overlap is
/// refused.
#[test]
fn a_client_mounts_the_layout_it_names() {
    let (first, second) = (FakeDevice::start(), FakeDevice::start());
    let proxy = Proxy::host(options()).unwrap();
    let at = |prefix: &str| prefix.parse::<DeviceRoute>().unwrap();
    let layout = Layout {
        mounts: [(&first, "/1"), (&second, "/2")]
            .map(|(device, prefix)| Mount {
                url: device.url.clone(),
                source: DeviceRoute::root(),
                prefix: at(prefix),
            })
            .to_vec(),
    };
    let client = TestClient::connect(&proxy.endpoint());
    client.hello(4242, "mounting");

    let reply = client.control("proxy.mount", &encode(&layout), 2);

    assert_eq!(
        reply.map(|reply| decode::<String>(&reply).unwrap()),
        Ok(layout.to_string())
    );
    assert_eq!(client.dev_name_at(at("/1"), 7), "FAKE");
    assert_eq!(client.dev_name_at(at("/2"), 8), "FAKE");
    let targets: Vec<_> = proxy
        .clients()
        .into_iter()
        .map(|client| client.target)
        .collect();
    assert_eq!(targets, [Some(layout.to_string())]);

    let overlapping = Layout {
        mounts: vec![
            layout.mounts[0].clone(),
            Mount {
                prefix: at("/1/2"),
                ..layout.mounts[1].clone()
            },
        ],
    };
    let other = TestClient::connect(&proxy.endpoint());
    other.hello(4242, "overlapping");
    let refused = other.control("proxy.mount", &encode(&overlapping), 2);
    assert_eq!(refused.unwrap_err().0, RpcError::Invalid);
}

/// A link still waiting for its device to answer is no candidate for
/// `auto`, so it leaves the one that answered the only choice.
#[test]
fn auto_counts_only_the_links_that_answered() {
    let device = FakeDevice::start();
    // It takes connections but never says anything.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
    let proxy = Proxy::host(options()).unwrap();
    let answered = TestClient::connect(&proxy.endpoint());
    answered.hello(4242, "answered");
    answered.open(&device.url).unwrap();
    let waiting = TestClient::connect(&proxy.endpoint());
    waiting.hello(4243, "waiting");
    let open = Packet::proxy_request("proxy.open", &encode(&silent_url), 2);
    waiting.send(open.unwrap());
    let deadline = Instant::now() + WAIT;
    while proxy.links.urls().len() < 2 {
        assert!(Instant::now() < deadline, "the silent link never opened");
        std::thread::sleep(Duration::from_millis(10));
    }

    let auto = TestClient::connect(&proxy.endpoint());
    auto.hello(4244, "auto");
    assert_eq!(auto.open("auto"), Ok(device.url.clone()));
    assert_eq!(auto.dev_name(7), "FAKE");
}

/// `auto` shares the link it chose as it is. A replug that moved the
/// device to another port leaves the link's URL naming another device, or
/// none, so the URL must not be looked up again.
#[test]
fn auto_shares_the_link_it_chose_though_its_url_no_longer_finds_it() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(options()).unwrap();
    let opener = TestClient::connect(&proxy.endpoint());
    opener.hello(4242, "opener");
    opener.open(&device.url).unwrap();

    proxy.links.rekey(&device.url, "usb:moved");

    let auto = TestClient::connect(&proxy.endpoint());
    auto.hello(4243, "auto");
    assert_eq!(auto.open("auto"), Ok(device.url.clone()));
    assert_eq!(auto.dev_name(7), "FAKE");
    assert_eq!(device.accepted(), 1);
    assert_eq!(proxy.links.users(), 2);
}

/// A client that hangs up while its open or probe waits for the device
/// is let go at once, rather than when the device would have had to
/// answer. Its task ends, and its use of the link with it.
#[test]
fn a_client_that_hangs_up_mid_wait_is_let_go() {
    // It takes connections but never says anything.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
    let proxy = Proxy::host(options()).unwrap();

    for method in ["proxy.open", "proxy.probe"] {
        let leaving = TestClient::connect(&proxy.endpoint());
        leaving.hello(4242, "leaving");
        let open = Packet::proxy_request(method, &encode(&silent_url), 2);
        leaving.send(open.unwrap());
        let deadline = Instant::now() + WAIT;
        while proxy.links.users() == 0 {
            assert!(Instant::now() < deadline, "{method} never began to wait");
            std::thread::sleep(Duration::from_millis(10));
        }

        drop(leaving);

        let deadline = Instant::now() + Duration::from_millis(1500);
        while !proxy.clients().is_empty() || proxy.links.users() > 0 {
            assert!(
                Instant::now() < deadline,
                "{method} still waits for a client that hung up"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// A packet that does not decode is skipped as the transport skips it,
/// whether it comes before the client's first control request or after
/// its open. The client stays connected, and is answered after it.
#[test]
fn a_packet_that_does_not_decode_is_skipped_and_the_client_stays() {
    use std::io::Write;

    let device = FakeDevice::start();
    let proxy = Proxy::host(options()).unwrap();
    let port: u16 = proxy
        .endpoint()
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let connect = || {
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).unwrap();
        (stream.try_clone().unwrap(), TestClient::over(stream))
    };
    // An RPC request too short for its header, and a type 64 packet
    // with no status in it.
    let undecodable = |raw: &mut TcpStream| {
        raw.write_all(&[2, 0, 1, 0, 0]).unwrap();
        raw.write_all(&[64, 0, 0, 0]).unwrap();
    };

    let (mut early_raw, early) = connect();
    undecodable(&mut early_raw);
    early.hello(4242, "early");
    early.open(&device.url).unwrap();
    assert_eq!(early.dev_name(7), "FAKE");

    let (mut late_raw, late) = connect();
    late.hello(4243, "late");
    late.open(&device.url).unwrap();
    undecodable(&mut late_raw);
    assert_eq!(late.dev_name(7), "FAKE");

    assert_eq!(proxy.clients().len(), 2);
}

/// A client whose first packet is not a control request opens nothing,
/// so what it sends reaches no device, though `auto` would resolve.
#[test]
fn a_client_that_opens_nothing_reaches_no_device() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(options()).unwrap();
    let opener = TestClient::connect(&proxy.endpoint());
    opener.hello(4242, "opener");
    opener.open(&device.url).unwrap();

    let talker = TestClient::connect(&proxy.endpoint());
    let request = Packet::rpc_request("dev.name", &[], 7, DeviceRoute::root());
    talker.send(request.unwrap());

    let deadline = Instant::now() + Duration::from_millis(500);
    while let Ok(heard) = talker.rx.recv_deadline(deadline) {
        if let Ok(transport::TransportEvent::Packet(pkt)) = heard {
            assert!(
                !matches!(pkt.payload(), Payload::RpcReply(_)),
                "a device answered"
            );
        }
    }
    assert_eq!(device.accepted(), 1);
}

/// A proxy refuses to open its own port at a loopback address or at
/// `localhost`, as its link would be a client of the proxy itself.
#[test]
fn a_proxy_refuses_to_mount_itself() {
    let proxy = Proxy::host(options()).unwrap();
    let client = TestClient::connect(&proxy.endpoint());
    client.hello(4242, "itself");
    let port = proxy.endpoint().rsplit(':').next().unwrap().to_string();

    for url in [proxy.endpoint(), format!("tcp://localhost:{port}")] {
        let (code, message) = client.open(&url).unwrap_err();
        assert_eq!(code, RpcError::Invalid);
        assert_eq!(
            message,
            format!("{url} is this proxy, which cannot mount itself")
        );
    }
    assert!(proxy.links.urls().is_empty(), "a link to the proxy opened");
}

/// Type 64 is also `USER`, which a device and an app may send. Such a
/// packet is no status of the proxy's, so it passes untouched both
/// ways, but for its route.
#[test]
fn a_user_packet_passes_through_untouched() {
    let device = FakeDevice::start();
    let prefix: DeviceRoute = "/3".parse().unwrap();
    let layout = Layout {
        mounts: vec![Mount {
            url: device.url.clone(),
            source: DeviceRoute::root(),
            prefix,
        }],
    };
    let proxy = Proxy::host(HostOptions {
        default: Some(layout),
        ..options()
    })
    .unwrap();
    let control = TestClient::connect(&proxy.endpoint());
    control.hello(4242, "control");
    control.open("default").unwrap();
    let auto = TestClient::connect(&proxy.endpoint());
    auto.hello(4243, "auto");
    auto.open("auto").unwrap();
    assert_eq!(auto.dev_name_at(prefix, 7), "FAKE");
    let (user, _) = Packet::from_slice_prefix(&[64, 0, 3, 0, 4, 0xAA, 0xBB]).unwrap();

    device.send(user.clone());

    for client in [&control, &auto] {
        client.expect(|pkt| match pkt.payload() {
            Payload::ProxyRx(rx) => panic!("the USER packet arrived as {rx:?}"),
            _ if pkt.ptype() == PacketType::PROXY_STATUS => {
                assert_eq!(pkt.as_bytes(), user.with_route(prefix).as_bytes());
                Some(())
            }
            _ => None,
        });
    }

    for client in [&control, &auto] {
        client.send(user.with_route(prefix));
    }

    for pkt in device.expect_received(PacketType::PROXY_STATUS, 2) {
        assert_eq!(pkt.as_bytes(), user.as_bytes());
    }
}

/// Whether `proxy` stops within the time a test waits, waited for on
/// another thread so that a proxy that never stops fails the test.
fn stops_in_time(proxy: Arc<Proxy>) -> bool {
    let (done, stopped) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        proxy.wait();
        let _ = done.send(());
    });
    stopped.recv_timeout(WAIT).is_ok()
}

/// Whether `proxy` shuts down within the time a test waits, shut down
/// on another thread so that a proxy that never stops fails the test.
fn shuts_down_in_time(proxy: Arc<Proxy>) -> bool {
    let (done, stopped) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        proxy.shutdown();
        let _ = done.send(());
    });
    stopped.recv_timeout(WAIT).is_ok()
}

/// A client's task that panics leaves the proxy serving the other
/// clients, the table without it, and the proxy able to stop.
#[test]
fn a_panic_in_a_client_task_wedges_nothing() {
    let device = FakeDevice::start();
    let proxy = Arc::new(Proxy::host(options()).unwrap());
    let first = TestClient::connect(&proxy.endpoint());
    first.hello(4242, "first");
    let panicking = TestClient::connect(&proxy.endpoint());
    panicking.hello(4243, "panicking");
    assert_eq!(panicking.open(&device.url), Ok(device.url.clone()));

    panicking.send(Packet::proxy_request("proxy.test.panic", &[], 3).unwrap());

    panicking.expect_closed();
    let second = TestClient::connect(&proxy.endpoint());
    second.hello(4244, "second");
    assert_eq!(second.open(&device.url), Ok(device.url.clone()));
    assert_eq!(second.dev_name(7), "FAKE");
    let deadline = Instant::now() + WAIT;
    while proxy.clients().len() > 2 {
        assert!(
            Instant::now() < deadline,
            "the client that panicked stayed listed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let names: Vec<_> = proxy
        .clients()
        .into_iter()
        .map(|client| client.name)
        .collect();
    assert_eq!(
        names,
        [Some("first".to_string()), Some("second".to_string())]
    );
    assert_eq!(proxy.links.users(), 1);

    assert!(shuts_down_in_time(proxy));
    first.expect_stopped();
    second.expect_stopped();
}

/// Hosts a proxy, and connects a client that opened `device` and one
/// that opened nothing. Returns the proxy and both clients.
fn hosted_with_clients(device: &FakeDevice) -> (Arc<Proxy>, TestClient, TestClient) {
    let proxy = Arc::new(Proxy::host(options()).unwrap());
    let user = TestClient::connect(&proxy.endpoint());
    user.hello(4242, "user");
    assert_eq!(user.open(&device.url), Ok(device.url.clone()));
    let watcher = TestClient::connect(&proxy.endpoint());
    watcher.hello(4243, "watcher");
    (proxy, user, watcher)
}

/// Asserts that `proxy` has stopped accepting clients and let go of
/// `device`.
fn assert_let_go(proxy: &Proxy, device: &FakeDevice) {
    let endpoint = proxy.endpoint();
    let connected = TcpStream::connect(endpoint.trim_start_matches("tcp://"));
    assert!(connected.is_err(), "the proxy still accepts clients");
    let deadline = Instant::now() + WAIT;
    while device.connections() > 0 {
        assert!(Instant::now() < deadline, "the device stayed open");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A panic in the registry of links stops the proxy. A client hears the
/// stop, waiters return, and the device is let go.
#[test]
fn a_panic_in_a_core_task_stops_the_proxy_cleanly() {
    let device = FakeDevice::start();
    let (proxy, user, watcher) = hosted_with_clients(&device);

    proxy.links.panic();

    watcher.expect_stopped();
    // Its link ended with the registry, before or after the stop.
    user.expect_closed();
    assert!(stops_in_time(proxy.clone()));
    assert_let_go(&proxy, &device);
}

/// A proxy that has shut down lists no client, and a second shutdown
/// does nothing.
#[test]
fn a_shutdown_proxy_lists_no_client_and_shuts_down_again_as_a_no_op() {
    let proxy = Proxy::host(options()).unwrap();
    let client = TestClient::connect(&proxy.endpoint());
    client.hello(4242, "client");
    assert_eq!(proxy.clients().len(), 1);

    proxy.shutdown();

    assert!(proxy.clients().is_empty(), "a client stayed listed");
    proxy.shutdown();
    proxy.wait();
}

/// A client still sending as the proxy stops hears the stop all the
/// same. Its connection closes only once what was queued for it is
/// written, however its own packets race the close.
#[test]
fn a_client_sending_as_the_proxy_stops_still_hears_the_stop() {
    use std::io::Write;

    let proxy = Proxy::host(options()).unwrap();
    let stream = TcpStream::connect(proxy.endpoint().trim_start_matches("tcp://")).unwrap();
    let mut raw = stream.try_clone().unwrap();
    let client = TestClient::over(stream);
    client.hello(4242, "sending");
    let beat = Packet::heartbeat(DeviceRoute::root());
    let sending = std::thread::spawn(move || while raw.write_all(beat.as_bytes()).is_ok() {});

    proxy.shutdown();

    client.expect_stopped();
    sending.join().unwrap();
}

/// A proxy hosted again on the port of one that just stopped binds it,
/// though the connections its clients closed still wait out their time
/// on the proxy's side.
#[test]
fn a_proxy_hosted_again_binds_the_port_its_stopped_clients_left() {
    let stopped = Proxy::host(options()).unwrap();
    let endpoint = stopped.endpoint();
    let clients: Vec<TestClient> = (0..4)
        .map(|n| {
            let client = TestClient::connect(&endpoint);
            client.hello(4242, &format!("client {n}"));
            client
        })
        .collect();
    stopped.shutdown();
    for client in &clients {
        client.expect_stopped();
    }
    drop(clients);

    let port = endpoint.rsplit(':').next().unwrap().parse().unwrap();
    let again = Proxy::host(HostOptions { port, ..options() });

    assert!(again.is_ok(), "{:?}", again.err());
}

/// A client whose open still waits for its device when the proxy stops
/// hears the stop, and never that its open was refused, which would blame
/// its target.
#[test]
fn an_open_still_waiting_hears_the_stop_and_no_refusal() {
    // It takes connections but never says anything.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
    let proxy = Arc::new(Proxy::host(options()).unwrap());
    let waiting = TestClient::connect(&proxy.endpoint());
    waiting.hello(4242, "waiting");
    let open = Packet::proxy_request("proxy.open", &encode(&silent_url), 2);
    waiting.send(open.unwrap());
    let deadline = Instant::now() + WAIT;
    while proxy.links.urls().is_empty() {
        assert!(Instant::now() < deadline, "the silent link never opened");
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(shuts_down_in_time(proxy));

    waiting.expect(|pkt| match pkt.payload() {
        Payload::ProxyRx(ProxyRx::Error(error)) => panic!(
            "the open was refused: {}",
            String::from_utf8_lossy(error.message)
        ),
        Payload::ProxyRx(ProxyRx::Link(status)) => {
            assert_eq!(status, LinkStatus::Stopped);
            assert_eq!(pkt.route(), DeviceRoute::root());
            Some(())
        }
        _ => None,
    });
    waiting.expect_closed();
}

/// A stop that the host refuses names the host, and a stop nothing
/// answers names the address.
#[test]
fn proxy_stop_is_accepted_only_when_the_host_is_stoppable() {
    let kept = Proxy::host(options()).unwrap();
    let refused = stop(Some(&kept.endpoint())).unwrap_err();
    assert_eq!(
        refused.to_string(),
        format!(
            "the proxy at {} is hosted by a test and stops only with it",
            kept.endpoint()
        )
    );
    let hello = caller_hello();
    assert_eq!(
        host_of(call(&kept.endpoint(), "proxy.hello", &hello, WAIT)),
        hosted_by("a test")
    );

    let stoppable = Proxy::host(HostOptions {
        stoppable: true,
        ..options()
    })
    .unwrap();
    let watcher = TestClient::connect(&stoppable.endpoint());
    watcher.hello(4242, "watcher");

    stop(Some(&stoppable.endpoint())).unwrap();

    watcher.expect_stopped();
    let endpoint = stoppable.endpoint();
    assert!(stops_in_time(Arc::new(stoppable)));
    assert_eq!(
        stop(Some(&endpoint)).unwrap_err().to_string(),
        format!("no proxy is running at {endpoint}")
    );
}

#[test]
fn clients_lists_who_said_hello_and_what_they_opened() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(options()).unwrap();
    let other = TestClient::connect(&proxy.endpoint());
    other.hello(4242, "other");
    other.open(&device.url).unwrap();
    let mine = TestClient::connect(&proxy.endpoint());
    mine.hello(std::process::id(), "mine");

    let clients = proxy.clients();

    let summary: Vec<_> = clients
        .iter()
        .map(|client| {
            (
                client.pid,
                client.name.as_deref(),
                client.version.as_deref(),
                client.target.as_deref(),
                client.own_process,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (
                Some(4242),
                Some("other"),
                Some(env!("CARGO_PKG_VERSION")),
                Some(device.url.as_str()),
                false
            ),
            (
                Some(std::process::id()),
                Some("mine"),
                Some(env!("CARGO_PKG_VERSION")),
                None,
                true
            ),
        ]
    );
    assert!(clients[0].id < clients[1].id);
    assert!(clients.iter().all(|client| client.peer.ip().is_loopback()));

    // A client leaves the table once its connection closes.
    drop(other);
    let deadline = Instant::now() + WAIT;
    while proxy.clients().len() > 1 {
        assert!(Instant::now() < deadline, "the closed client stayed listed");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(proxy.clients()[0].name.as_deref(), Some("mine"));
}

/// The layout that mounts each of `devices` at a prefix of its own, and
/// the prefixes.
fn each_at_its_own_prefix(devices: &[FakeDevice]) -> (Layout, Vec<DeviceRoute>) {
    let prefixes: Vec<DeviceRoute> = (1..=devices.len())
        .map(|place| format!("/{place}").parse().unwrap())
        .collect();
    let mounts = devices.iter().zip(&prefixes).map(|(device, prefix)| Mount {
        url: device.url.clone(),
        source: DeviceRoute::root(),
        prefix: *prefix,
    });
    let layout = Layout {
        mounts: mounts.collect(),
    };
    (layout, prefixes)
}

/// A proxy hosted and a connection opened through the async entry points
/// run every task on the runtime they are awaited on, here one of a
/// single thread, and never reach the crate's runtime. The proxy keeps
/// the tables and settings of several devices there.
#[test]
fn a_proxy_and_a_connection_run_on_the_runtime_they_are_awaited_on() {
    let devices: Vec<FakeDevice> = (0..3).map(|_| FakeDevice::start()).collect();
    let (layout, prefixes) = each_at_its_own_prefix(&devices);
    let caller = std::thread::spawn(move || {
        rt::forbid_crate_runtime();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_start(rt::forbid_crate_runtime)
            .build()
            .unwrap();
        runtime.block_on(async {
            let proxy = host(options()).await.unwrap();
            let address = crate::session::ProxyAddress {
                url: proxy.endpoint(),
                ensure: None,
            };
            let session = crate::session::Session::mount_async(&address, &layout, WAIT);
            let connection = crate::Connection::over(session.await.unwrap());
            for prefix in &prefixes {
                let device = connection.device(*prefix);
                assert_eq!(device.submit("dev.name", &[]).await.unwrap(), b"FAKE");
                let followed = device.settings_updates();
                let settings = tokio::time::timeout(WAIT, followed.recv_async()).await;
                let settings = settings.expect("the settings in time").unwrap();
                assert_eq!(settings.get("test.amplitude"), Some(&[1u8][..]));
            }
            connection.close();
            proxy.shutdown_async().await;
        });
    });
    if let Err(panic) = caller.join() {
        std::panic::resume_unwind(panic);
    }
}

/// The synchronous API works from inside an application's tokio runtime
/// of one thread, where `block_on` panics. It runs on the crate's runtime
/// and blocks the application's thread until each call is done.
#[tokio::test]
async fn the_sync_api_works_inside_an_applications_runtime() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(HostOptions {
        default: Some(Layout::single(&device.url)),
        ..options()
    })
    .unwrap();
    let connection = crate::Connection::open(&proxy.endpoint()).unwrap();

    let name: String = connection
        .device(DeviceRoute::root())
        .get("dev.name")
        .unwrap();

    assert_eq!(name, "FAKE");
    drop(connection);
    drop(proxy);
}

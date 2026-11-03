//! Test fixtures for `device`, a proxy on a free port and the fake device
//! of the proxy's own tests, and the packets a device sends.

use crate::device::session::ProxyAddress;
use crate::device::session::Session;
use crate::device::stream::Receiver;
use crate::device::Connection;
use crate::proto::data::{self as wire, DataType};
use crate::proto::sync::Epoch;
use crate::proto::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
pub(crate) use crate::proxy::tests::fixtures::{attached, FakeDevice, WAIT};
use crate::proxy::{tests::fixtures, Layout, Mount, Proxy, Purpose};
use crate::tio::packet::Packet;
use std::io;
use std::net::{Ipv4Addr, TcpListener};
use std::sync::Mutex;

/// Held by each test that sets the variables a proxy is found or started by,
/// such as `TWINLEAF_PROXY`, `TWINLEAF_TIO` and `TWINLEAF_RUNTIME_DIR`. Every
/// test in the process shares them.
pub(crate) static ENVIRONMENT: Mutex<()> = Mutex::new(());

/// Hosts a proxy on a free port of this machine, with no default.
pub(crate) fn proxy() -> Proxy {
    Proxy::host(fixtures::options()).unwrap()
}

/// Hosts a proxy on a free port of this machine, with the whole of the device
/// at `url` as its default.
pub(crate) fn proxy_serving(url: &str) -> Proxy {
    let proxy = proxy();
    proxy.set_default(Some(Layout {
        mounts: vec![Mount {
            url: url.to_string(),
            source: DeviceRoute::root(),
            prefix: DeviceRoute::root(),
        }],
    }));
    proxy
}

/// Opens `url` through `proxy`, as [`Connection::open`] does through the
/// proxy `TWINLEAF_PROXY` names. The proxy opens the fake devices' loopback
/// addresses as it opens any device.
pub(crate) fn open_through(proxy: &Proxy, url: &str) -> io::Result<Connection> {
    let proxy = ProxyAddress {
        url: proxy.endpoint(),
        ensure: None,
    };
    Session::open(&proxy, url, Purpose::Use, WAIT).map(Connection::over)
}

/// A port of `127.0.0.1` that nothing listens on, so it refuses connections
/// unless another process takes it after it was found free.
pub(crate) struct ClosedPort(u16);

impl ClosedPort {
    /// Finds a free port and leaves it unbound.
    pub(crate) fn new() -> ClosedPort {
        ClosedPort(
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
        )
    }

    /// Returns the port number.
    pub(crate) fn port(&self) -> u16 {
        self.0
    }

    /// Returns its URL, such as `tcp://127.0.0.1:40000`.
    pub(crate) fn url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port())
    }

    /// Starts listening on the port.
    #[cfg(unix)]
    pub(crate) fn listen(self) -> TcpListener {
        TcpListener::bind((Ipv4Addr::LOCALHOST, self.0)).unwrap()
    }
}

/// How many ports [`on_free_port`] tries.
const PORT_ATTEMPTS: usize = 5;

/// Runs `scenario` on a port of `127.0.0.1` that was free a moment before,
/// for a test whose proxy binds the port itself.
///
/// Another process can take the port in that moment. When `scenario` returns
/// `None` and the port is held elsewhere, the scenario runs again on another
/// port. Returns `None` if the port was free and the scenario still failed,
/// or every port tried was taken.
pub(crate) fn on_free_port<T>(mut scenario: impl FnMut(u16) -> Option<T>) -> Option<T> {
    for _ in 0..PORT_ATTEMPTS {
        let port = ClosedPort::new().port();
        if let Some(done) = scenario(port) {
            return Some(done);
        }
        let taken = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .is_err_and(|error| error.kind() == io::ErrorKind::AddrInUse);
        if !taken {
            return None;
        }
    }
    None
}

/// A packet of one f32 sample of stream 1 at `route`, numbered `first`.
pub(crate) fn samples(first: u32, route: DeviceRoute) -> Packet {
    Packet::samples(1, 0, first, &[0; 4], route).expect("valid samples")
}

/// One SETTING announcement, as a device sends it.
pub(crate) fn announcement(
    name: &str,
    seq: Option<u32>,
    reply: &[u8],
    route: DeviceRoute,
) -> Packet {
    let mut raw = [0u8; 64];
    let len = crate::proto::settings::Setting {
        name: name.as_bytes(),
        seq,
        reply,
    }
    .write(&mut raw)
    .expect("the announcement fits");
    Packet::from_slice_prefix(&raw[..len])
        .expect("a valid packet")
        .0
        .with_route(route)
}

/// The four records that let a parser decode one f32 stream, as the device
/// in `session` describes them.
pub(crate) fn metadata_records(session: u32) -> [wire::Metadata<'static>; 4] {
    [
        wire::Metadata::Device(wire::Device {
            session: SessionId::new(session),
            n_streams: 1,
            name: "d",
            serial: "s",
            firmware: "f",
        }),
        wire::Metadata::Stream(wire::Stream {
            stream_id: StreamId::new(1),
            n_columns: 1,
            n_segments: 2,
            sample_size: 4,
            buf_samples: 128,
            name: "stream",
        }),
        wire::Metadata::Segment(segment_record(0)),
        wire::Metadata::Column(wire::Column {
            stream_id: StreamId::new(1),
            index: ColumnId::new(0),
            data_type: DataType::F32,
            name: "col",
            units: "",
            description: "",
        }),
    ]
}

/// The stream's segment `segment_id`, starting one second per segment in.
pub(crate) fn segment_record(segment_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id: StreamId::new(1),
        segment_id: SegmentId::new(segment_id),
        flags: wire::SegmentFlags::default(),
        epoch: Epoch::UNIX,
        timeref_serial: "clock",
        timeref_session: SessionId::new(7),
        start_time: u32::from(segment_id),
        sampling_rate: 1,
        decimation: 1,
        filter_cutoff: 0.0,
        filter_type: wire::FilterType::NONE,
    }
}

/// The metadata packet that broadcasts `record` at `route`.
pub(crate) fn broadcast(record: wire::Metadata<'_>, route: DeviceRoute) -> Packet {
    Packet::metadata(record, wire::MetadataFlags::UPDATE, route).expect("one record fits")
}

/// The packets that describe the device at `route` in `session`.
pub(crate) fn description(route: DeviceRoute, session: u32) -> Vec<Packet> {
    let records = metadata_records(session).into_iter();
    records.map(|record| broadcast(record, route)).collect()
}

/// One `dev.metadata` reply frame describing a one-stream device.
pub(crate) fn device_reply() -> Vec<u8> {
    let mut record = [0u8; 64];
    let len = metadata_records(1)[0]
        .write_reply_frame(&mut record)
        .expect("one record fits");
    record[..len].to_vec()
}

/// Takes items until one matches and returns it, or panics rather than
/// block forever.
pub(crate) fn wait_for<T>(items: &Receiver<T>, wanted: impl Fn(&T) -> bool) -> T {
    let deadline = std::time::Instant::now() + WAIT;
    while let Ok(item) = items.recv_deadline(deadline) {
        if wanted(&item) {
            return item;
        }
    }
    panic!("what the test waited for never came");
}

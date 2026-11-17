//! The server over real sockets: a sensor behind a TCP port, mounted and
//! served, and a client reading the outputs and setting a param, all through
//! the library a real client uses.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::proto::data::{self as wire, DataType};
use twinleaf::proto::rpc::Method;
use twinleaf::proto::sync::Epoch;
use twinleaf::tio::packet::Payload;
use twinleaf::tio::Packet;
use twinleaf::{ColumnId, Connection, DeviceRoute, SegmentId, SessionId, StreamId};
use twinleaf_dsp::run::RunOptions;
use twinleaf_dsp::serve::{MountSpec, ServeOptions, Server};

const RATE: u32 = 200;

/// A sensor behind a TCP port on this machine. Sample `n` of its one stream,
/// `field`, is `x = n` and `y = -n / 2`, so a reader can tell from any row
/// which sample it is and what was done to it.
struct FakeSensor {
    url: String,
    /// The whole second its sample 0 is at.
    start: u32,
    stop: Arc<AtomicBool>,
}

impl Drop for FakeSensor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl FakeSensor {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let stopping = Arc::clone(&stopping);
                        std::thread::spawn(move || serve(stream, start, &stopping));
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        Self { url, start, stop }
    }
}

fn records(start: u32) -> Vec<wire::Metadata<'static>> {
    let stream_id = StreamId::new(1);
    let column = |index: u8, name: &'static str| {
        wire::Metadata::Column(wire::Column {
            stream_id,
            index: ColumnId::new(index),
            data_type: DataType::F64,
            name,
            units: "nT",
            description: "",
        })
    };
    vec![
        wire::Metadata::Device(wire::Device {
            session: SessionId::new(77),
            n_streams: 1,
            name: "fake",
            serial: "FAKE1",
            firmware: "test",
            protocol: 0,
        }),
        wire::Metadata::Stream(wire::Stream {
            stream_id,
            n_columns: 2,
            n_segments: 2,
            sample_size: 16,
            buf_samples: 20,
            name: "field",
        }),
        wire::Metadata::Segment(wire::Segment {
            stream_id,
            segment_id: SegmentId::new(0),
            flags: wire::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "clock",
            timeref_session: SessionId::new(5),
            start_time: start,
            sampling_rate: RATE,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
        }),
        column(0, "x"),
        column(1, "y"),
    ]
}

/// One connection: describe the stream, then send samples as fast as a
/// sensor at `RATE` takes them, and answer what a client asks on connecting.
fn serve(mut stream: TcpStream, start: u32, stop: &AtomicBool) {
    let root = DeviceRoute::root();
    stream
        .set_read_timeout(Some(Duration::from_millis(5)))
        .unwrap();
    let mut reader = stream.try_clone().unwrap();
    let mut send = |packet: Packet| stream.write_all(packet.as_bytes()).is_ok();
    if !send(Packet::heartbeat(root)) {
        return;
    }
    for record in records(start) {
        if !send(Packet::metadata(record, wire::MetadataFlags::UPDATE, root).unwrap()) {
            return;
        }
    }
    let began = Instant::now();
    let mut sent = 0u32;
    let mut beat = Instant::now();
    let mut received = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        let due = (began.elapsed().as_secs_f64() * f64::from(RATE)) as u32;
        while sent < due {
            let count = (due - sent).min(20);
            let data: Vec<u8> = (sent..sent + count)
                .flat_map(|n| [f64::from(n), -f64::from(n) / 2.0])
                .flat_map(f64::to_le_bytes)
                .collect();
            if !send(Packet::samples(1, 0, sent, &data, root).unwrap()) {
                return;
            }
            sent += count;
        }
        if beat.elapsed() >= Duration::from_millis(200) {
            if !send(Packet::heartbeat(root)) {
                return;
            }
            beat = Instant::now();
        }
        let mut chunk = [0u8; 1024];
        match reader.read(&mut chunk) {
            Ok(0) => return,
            Ok(count) => received.extend_from_slice(&chunk[..count]),
            Err(_) => {}
        }
        while let Ok((packet, used)) = Packet::from_slice_prefix(&received) {
            received.drain(..used);
            let Payload::RpcRequest(request) = packet.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            let value = match name {
                b"dev.name" => b"fake".to_vec(),
                b"rpc.hash" => 0xFA4Eu32.to_le_bytes().to_vec(),
                b"dev.session" | b"settings.version" => 77u32.to_le_bytes().to_vec(),
                b"rpc.listinfo" if request.args.is_empty() => 0u16.to_le_bytes().to_vec(),
                b"dev.metadata" => {
                    let mut reply = vec![0u8; wire::MAX_METADATA_REPLY_SIZE];
                    let written = records(start).iter().fold(0, |written, record| {
                        written + record.write_reply_frame(&mut reply[written..]).unwrap()
                    });
                    reply.truncate(written);
                    reply
                }
                _ => continue,
            };
            let reply = Packet::rpc_reply(request.id.value(), &value, packet.route()).unwrap();
            if !send(reply) {
                return;
            }
        }
    }
}

const SPEC: &str = "\
source a = /0/field @rate(200) @channels(x, y) @unit(nT)

param gain = 2 [1 .. 4]

out scaled : nT = a * gain
";

#[test]
fn a_client_reads_the_outputs_and_sets_a_param_through_the_served_tree() {
    let sensor = FakeSensor::start();
    let server = Server::start(
        SPEC.to_owned(),
        ServeOptions {
            mounts: vec![MountSpec {
                url: sensor.url.clone(),
                prefix: "/0".parse().unwrap(),
                source: DeviceRoute::root(),
            }],
            port: 0,
            all_interfaces: false,
            run: RunOptions::default(),
        },
    )
    .unwrap();
    assert_eq!(server.route().to_string(), "/255");

    // A local URL opens the whole tree: the sensor at /0 and the outputs.
    let connection = Connection::open(server.endpoint()).unwrap();
    let outputs = connection.device(server.route());
    let samples = outputs.samples();

    // What gain each row was computed with, read off the row itself: sample
    // n is at `start + n / RATE`, and the output's x is `gain * n`.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut gains = Vec::new();
    let mut set = false;
    while Instant::now() < deadline {
        let Ok(batch) = samples.recv_timeout(Duration::from_millis(500)) else {
            continue;
        };
        let metadata = batch.metadata();
        assert_eq!(metadata.schema().stream().name(), "scaled");
        assert_eq!(metadata.rate_hz(), f64::from(RATE));
        // The output tells time by its source's clock.
        assert_eq!(metadata.segment().timeref_serial(), "clock");
        let columns: Vec<_> = batch.columns().iter().map(|c| c.to_f64()).collect();
        for (row, (x, y)) in columns[0].iter().zip(columns[1].iter()).enumerate() {
            let (x, y) = (*x, *y);
            let time = batch.row(row).unwrap().timestamp_begin();
            let n = ((time - f64::from(sensor.start)) * f64::from(RATE)).round();
            assert_eq!(x, -2.0 * y, "the row at {time} is not one sample");
            if n > 0.0 {
                gains.push(x / n);
            }
        }
        // Once the outputs are flowing, set the gain over RPC.
        if !set && gains.len() > 50 {
            let gain = |reply: Vec<u8>| f64::from_le_bytes(reply.try_into().unwrap());
            assert_eq!(gain(outputs.raw_rpc("dsp.gain", &[]).unwrap()), 2.0);
            assert_eq!(gain(outputs.raw_rpc("dsp.gain.max", &[]).unwrap()), 4.0);
            let written = outputs.raw_rpc("dsp.gain", &3.0_f64.to_le_bytes()).unwrap();
            assert_eq!(gain(written), 3.0);
            assert!(outputs.raw_rpc("dsp.gain", &9.0_f64.to_le_bytes()).is_err());
            set = true;
        }
        if set && gains.iter().filter(|gain| **gain == 3.0).count() > 50 {
            break;
        }
    }
    assert!(set, "no output arrived: {} rows", gains.len());
    // Every row was computed with one gain or the other, and the change
    // came after the first rows and before the last.
    assert!(
        gains.iter().all(|gain| *gain == 2.0 || *gain == 3.0),
        "{gains:?}"
    );
    assert_eq!(gains[0], 2.0);
    assert!(
        gains.iter().filter(|gain| **gain == 3.0).count() > 50,
        "{gains:?}"
    );

    // The spec the device hands back is the one now running.
    let text = outputs
        .raw_rpc("dsp.spec.read", &0u32.to_le_bytes())
        .unwrap();
    assert_eq!(
        String::from_utf8(text).unwrap(),
        SPEC.replace("gain = 2", "gain = 3")
    );

    // Dropping the server stops serving.
    let endpoint = server.endpoint().to_owned();
    drop(connection);
    drop(server);
    assert!(TcpStream::connect(endpoint.trim_start_matches("tcp://")).is_err());
}

/// Not a test of anything: a measurement, run by hand with
/// `cargo test -p twinleaf-dsp --release --test server -- --ignored --nocapture`.
///
/// How much later than a sensor's sample a client sees the output computed
/// from it, both read through the one served tree. The spec adds no latency
/// of its own, so what is left is the path: the sample is read back through
/// the proxy, computed on, published by the device over loopback, and
/// forwarded by the proxy again.
#[test]
#[ignore = "a measurement, not a check"]
fn measure_how_much_later_an_output_arrives_than_its_sample() {
    let sensor = FakeSensor::start();
    let server = Server::start(
        SPEC.to_owned(),
        ServeOptions {
            mounts: vec![MountSpec {
                url: sensor.url.clone(),
                prefix: "/0".parse().unwrap(),
                source: DeviceRoute::root(),
            }],
            port: 0,
            all_interfaces: false,
            run: RunOptions::default(),
        },
    )
    .unwrap();
    let connection = Connection::open(server.endpoint()).unwrap();
    let all = connection.samples();
    let sample_of = |time: f64| ((time - f64::from(sensor.start)) * f64::from(RATE)).round() as i64;

    let mut seen = std::collections::HashMap::new();
    let mut later = Vec::new();
    let until = Instant::now() + Duration::from_secs(6);
    while Instant::now() < until {
        let Ok(batch) = all.recv_timeout(Duration::from_millis(500)) else {
            continue;
        };
        let now = Instant::now();
        let output = batch.route() == server.route();
        for row in 0..batch.len() {
            let n = sample_of(batch.row(row).unwrap().timestamp_begin());
            if output {
                if let Some(arrived) = seen.remove(&n) {
                    later.push(now.duration_since(arrived).as_secs_f64() * 1e3);
                }
            } else {
                seen.insert(n, now);
            }
        }
    }
    later.sort_by(f64::total_cmp);
    assert!(
        later.len() > 500,
        "too few rows to measure: {}",
        later.len()
    );
    let at = |fraction: f64| later[((later.len() - 1) as f64 * fraction) as usize];
    println!(
        "an output arrives after its sample by: median {:.2} ms, 95th percentile {:.2} ms, most {:.2} ms, over {} samples",
        at(0.5),
        at(0.95),
        at(1.0),
        later.len()
    );
}

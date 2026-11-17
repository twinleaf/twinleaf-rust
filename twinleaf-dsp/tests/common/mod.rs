//! Recordings for tests, synthesized packet by packet, as two sensors on one
//! time base would write them.

#![allow(dead_code)]

use std::f64::consts::PI;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use twinleaf::data::{LogFile, PacketParser, SampleBatch};
use twinleaf::proto::data::{self as wire, DataType};
use twinleaf::proto::sync::Epoch;
use twinleaf::tio::Packet;
use twinleaf::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};

pub const FAST: u32 = 1000;
pub const SLOW: u32 = 500;
/// How much of the common field each axis sees.
pub const COMMON: [f64; 3] = [1.0, 0.5, -0.25];
/// A source near the first sensor only.
pub const LOCAL: [f64; 3] = [3.0, 0.0, 1.5];
/// Samples per packet: twenty rows of three doubles fill one.
pub const PACKET: usize = 20;

pub fn noise(seed: u64, channel: u64, n: u64) -> f64 {
    ((n * 2_654_435_761 + channel * 40_503 + seed * 7919) % 1_000_003) as f64 / 1_000_003.0 - 0.5
}

pub fn common(t: f64) -> f64 {
    50.0 * (2.0 * PI * 7.0 * t).sin() + 20.0 * (2.0 * PI * 60.0 * t).sin()
}

/// The first sensor: the common field, a local source, and a little noise.
pub fn first_sensor(samples: usize, local: f64, noisy: f64) -> Vec<[f64; 3]> {
    (0..samples as u64)
        .map(|n| {
            let t = n as f64 / f64::from(FAST);
            [0, 1, 2].map(|c| {
                let near = local * LOCAL[c] * (2.0 * PI * 11.0 * t).sin();
                COMMON[c] * common(t) + near + noisy * noise(1, c as u64, n)
            })
        })
        .collect()
}

/// The second sensor: the common field and a little noise.
pub fn second_sensor(samples: usize, noisy: f64) -> Vec<[f64; 3]> {
    (0..samples as u64)
        .map(|n| {
            let t = n as f64 / f64::from(SLOW);
            [0, 1, 2].map(|c| COMMON[c] * common(t) + noisy * noise(2, c as u64, n))
        })
        .collect()
}

/// One sensor's part of a recording.
pub struct Sensor<'a> {
    pub route: &'a str,
    pub rate: u32,
    /// The serial and session of the clock its time is told by.
    pub clock: (&'a str, u32),
    pub columns: [&'a str; 3],
    pub units: &'a str,
    pub rows: Vec<[f64; 3]>,
    /// Packets left out, to leave gaps.
    pub dropped: &'a [usize],
}

impl<'a> Sensor<'a> {
    pub fn new(route: &'a str, rate: u32, rows: Vec<[f64; 3]>) -> Self {
        Self {
            route,
            rate,
            clock: ("hub", 1),
            columns: ["x", "y", "z"],
            units: "nT",
            rows,
            dropped: &[],
        }
    }
}

/// Writes a recording: each sensor's metadata, then every sensor's packets
/// in the order of the time they cover.
pub fn record(name: &str, sensors: &[Sensor<'_>]) -> PathBuf {
    let stream = StreamId::new(1);
    let mut packets: Vec<(f64, usize, Packet)> = Vec::new();
    let mut file_bytes = Vec::new();
    for (index, sensor) in sensors.iter().enumerate() {
        let route: DeviceRoute = sensor.route.parse().unwrap();
        let mut records = vec![
            wire::Metadata::Device(wire::Device {
                session: SessionId::new(40 + index as u32),
                n_streams: 1,
                name: "sensor",
                serial: sensor.route,
                firmware: "test",
                protocol: 0,
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: stream,
                n_columns: 3,
                n_segments: 1,
                sample_size: 24,
                buf_samples: 128,
                name: "field",
            }),
            wire::Metadata::Segment(wire::Segment {
                stream_id: stream,
                segment_id: SegmentId::new(0),
                flags: wire::SegmentFlags::default(),
                epoch: Epoch::UNIX,
                timeref_serial: sensor.clock.0,
                timeref_session: SessionId::new(sensor.clock.1),
                start_time: 1_700_000_000,
                sampling_rate: sensor.rate,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: wire::FilterType::NONE,
            }),
        ];
        for (column, name) in sensor.columns.into_iter().enumerate() {
            records.push(wire::Metadata::Column(wire::Column {
                stream_id: stream,
                index: ColumnId::new(column as u8),
                data_type: DataType::F64,
                name,
                units: sensor.units,
                description: "",
            }));
        }
        for record in records {
            let packet = Packet::metadata(record, wire::MetadataFlags::UPDATE, route).unwrap();
            file_bytes.extend_from_slice(packet.as_bytes());
        }
        for (number, rows) in sensor.rows.chunks(PACKET).enumerate() {
            if sensor.dropped.contains(&number) {
                continue;
            }
            let data: Vec<u8> = rows
                .iter()
                .flatten()
                .flat_map(|value| value.to_le_bytes())
                .collect();
            let first = (number * PACKET) as u32;
            let packet = Packet::samples(1, 0, first, &data, route).unwrap();
            // A packet is written once its last sample has been taken.
            let written = (first as usize + rows.len()) as f64 / f64::from(sensor.rate);
            packets.push((written, index, packet));
        }
    }
    packets.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    for (_, _, packet) in &packets {
        file_bytes.extend_from_slice(packet.as_bytes());
    }
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}.tio"));
    fs::File::create(&path)
        .unwrap()
        .write_all(&file_bytes)
        .unwrap();
    path
}

/// Every batch of a recording, in the order its packets were written.
pub fn batches(log: &Path) -> Vec<SampleBatch> {
    let log = LogFile::open(log).unwrap();
    let mut parser = PacketParser::new(false);
    let mut batches = Vec::new();
    for packet in log.packets() {
        parser.push_packet(&packet.unwrap()).unwrap();
        parser.flush();
        while let Some(batch) = parser.pop_batch() {
            batches.push(batch);
        }
    }
    batches
}

/// Runs one command line in this process, as `tio dsp` would, and returns
/// its exit status with what it wrote on each stream. A command line that
/// does not parse has clap's status and message.
#[cfg(feature = "cli")]
pub fn command(args: &[&str]) -> (u8, String, String) {
    use twinleaf_dsp::cli::{self, Console};
    let library = twinleaf_dsp::Library::standard();
    let line = std::iter::once("tio dsp").chain(args.iter().copied());
    let cli = match cli::parse(&library, line) {
        Ok(cli) => cli,
        Err(error) => {
            let status = u8::try_from(error.exit_code()).unwrap();
            let text = error.to_string();
            return match error.use_stderr() {
                true => (status, String::new(), text),
                false => (status, text, String::new()),
            };
        }
    };
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let status = cli::execute(
        &library,
        cli,
        &mut Console {
            out: &mut out,
            err: &mut err,
        },
    );
    (
        status,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

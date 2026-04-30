//! tio test
//!
//! Simulates a small Twinleaf device that publishes a noisy sine wave on stream 1.

use crate::TestCli;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::tio::proto;
use twinleaf::tio::proto::meta;

const STREAM_ID: u8 = 1;
const N_SEGMENTS: u8 = 16;
const RPC_HASH: u32 = 0x7465_7374;
const DEVICE_NAME: &str = "tio-test";
const DEVICE_SERIAL: &str = "SIM0001";
const DEVICE_FIRMWARE: &str = "twinleaf-rust-test";
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_SAMPLES_PER_PACKET: u64 = 48;
const MAX_SAMPLE_NUMBER: u32 = 0x00ff_ffff;

const META_F64_RW: u16 = 0x0100 | 0x0200 | (8 << 4) | 2;
const META_U32_R: u16 = 0x0100 | (4 << 4) | 0;
const META_STRING_R: u16 = 0x0100 | 3;
const META_RAW: u16 = 0;

#[derive(Clone, Copy)]
struct SineParams {
    amplitude: f64,
    frequency: f64,
    noise: f64,
}

#[derive(Clone)]
struct RpcSpec {
    name: &'static str,
    meta: u16,
}

#[derive(Clone, Copy)]
struct Client {
    addr: SocketAddr,
    last_rx: Instant,
}

struct GaussianRng {
    state: u64,
    cached: Option<f64>,
}

impl GaussianRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed,
            cached: None,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn next_unit(&mut self) -> f64 {
        let raw = self.next_u64() >> 11;
        ((raw as f64) + 1.0) / ((1u64 << 53) as f64 + 1.0)
    }

    fn next_gaussian(&mut self) -> f64 {
        if let Some(value) = self.cached.take() {
            return value;
        }

        let u1 = self.next_unit();
        let u2 = self.next_unit();
        let radius = (-2.0 * u1.ln()).sqrt();
        let phase = std::f64::consts::TAU * u2;
        self.cached = Some(radius * phase.sin());
        radius * phase.cos()
    }
}

struct TestDevice {
    socket: UdpSocket,
    client: Option<Client>,
    params: SineParams,
    sample_rate: u32,
    segment_seconds: u32,
    segment_samples: u32,
    session_id: u32,
    started_at: Instant,
    start_time: u32,
    samples_generated: u64,
    sample_number: u32,
    segment_id: u8,
    segment_start_time: u32,
    pending_segment_update: bool,
    last_heartbeat: Instant,
    rng: GaussianRng,
    rpcs: Vec<RpcSpec>,
}

impl TestDevice {
    fn new(cli: TestCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;

        let now = unix_duration();
        let start_time = unix_time_secs(now);
        let seed = now.as_nanos() as u64 ^ u64::from(cli.port).rotate_left(32);
        let session_id = (seed as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        let segment_samples = cli
            .samplerate
            .checked_mul(cli.segment_seconds)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "segment too long"))?;
        if segment_samples > MAX_SAMPLE_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment contains too many samples for TIO sample numbering",
            ));
        }

        Ok(Self {
            socket,
            client: None,
            params: SineParams {
                amplitude: cli.amplitude,
                frequency: cli.frequency,
                noise: cli.noise,
            },
            sample_rate: cli.samplerate,
            segment_seconds: cli.segment_seconds,
            segment_samples,
            session_id,
            started_at: Instant::now(),
            start_time,
            samples_generated: 0,
            sample_number: 0,
            segment_id: 0,
            segment_start_time: start_time,
            pending_segment_update: false,
            last_heartbeat: Instant::now(),
            rng: GaussianRng::new(seed | 1),
            rpcs: vec![
                RpcSpec {
                    name: "dev.name",
                    meta: META_STRING_R,
                },
                RpcSpec {
                    name: "rpc.hash",
                    meta: META_U32_R,
                },
                RpcSpec {
                    name: "rpc.info",
                    meta: META_RAW,
                },
                RpcSpec {
                    name: "rpc.listinfo",
                    meta: META_RAW,
                },
                RpcSpec {
                    name: "dev.metadata",
                    meta: META_RAW,
                },
                RpcSpec {
                    name: "test.amplitude",
                    meta: META_F64_RW,
                },
                RpcSpec {
                    name: "test.frequency",
                    meta: META_F64_RW,
                },
                RpcSpec {
                    name: "test.noise",
                    meta: META_F64_RW,
                },
            ],
        })
    }

    fn run(&mut self) -> io::Result<()> {
        println!(
            "tio test listening on udp://0.0.0.0:{}",
            self.socket.local_addr()?.port()
        );
        println!(
            "  stream 1: amplitude={} V frequency={} Hz noise={} V/sqrt(Hz) samplerate={} Hz segment={} s",
            self.params.amplitude,
            self.params.frequency,
            self.params.noise,
            self.sample_rate,
            self.segment_seconds
        );
        println!(
            "  connect with: tio proxy udp4://127.0.0.1:{}",
            self.socket.local_addr()?.port()
        );

        loop {
            self.receive_packets()?;
            self.expire_client();
            self.send_periodic_packets()?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn receive_packets(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 1024];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((size, addr)) => {
                    if !self.accept_packet_from(addr)? {
                        continue;
                    }
                    match proto::Packet::deserialize(&buf[..size]) {
                        Ok((packet, parsed_size)) if parsed_size == size => {
                            self.handle_packet(packet, addr)?;
                        }
                        Ok(_) => {
                            eprintln!("Ignoring UDP datagram with trailing bytes from {addr}");
                        }
                        Err(err) => {
                            eprintln!("Ignoring malformed packet from {addr}: {err:?}");
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) => return Err(err),
            }
        }
    }

    fn accept_packet_from(&mut self, addr: SocketAddr) -> io::Result<bool> {
        let now = Instant::now();
        match self.client {
            Some(mut client) if client.addr == addr => {
                client.last_rx = now;
                self.client = Some(client);
                Ok(true)
            }
            Some(client) if now.duration_since(client.last_rx) < CLIENT_TIMEOUT => Ok(false),
            _ => {
                self.client = Some(Client { addr, last_rx: now });
                self.reset_run();
                println!("client connected: {addr}");
                self.send_initial_packets(addr)?;
                Ok(true)
            }
        }
    }

    fn expire_client(&mut self) {
        if let Some(client) = self.client {
            if Instant::now().duration_since(client.last_rx) > CLIENT_TIMEOUT {
                println!("client disconnected: {}", client.addr);
                self.client = None;
            }
        }
    }

    fn reset_run(&mut self) {
        self.started_at = Instant::now();
        self.start_time = unix_time_secs(unix_duration());
        self.samples_generated = 0;
        self.sample_number = 0;
        self.segment_id = 0;
        self.segment_start_time = self.start_time;
        self.pending_segment_update = false;
        self.last_heartbeat = Instant::now()
            .checked_sub(HEARTBEAT_INTERVAL)
            .unwrap_or_else(Instant::now);
    }

    fn handle_packet(&mut self, packet: proto::Packet, addr: SocketAddr) -> io::Result<()> {
        if let proto::Payload::RpcRequest(req) = packet.payload {
            self.handle_rpc(req, packet.routing, addr)?;
        }
        Ok(())
    }

    fn handle_rpc(
        &mut self,
        req: proto::RpcRequestPayload,
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let method = match &req.method {
            proto::RpcMethod::Name(name) => name.as_str(),
            proto::RpcMethod::Id(_) => {
                return self.send_rpc_error(req.id, proto::RpcErrorCode::NotFound, routing, addr)
            }
        };

        let result = match method {
            "dev.name" => self.rpc_read_string(req.id, DEVICE_NAME, &req.arg, routing, addr),
            "rpc.hash" => self.rpc_read_u32(req.id, RPC_HASH, &req.arg, routing, addr),
            "rpc.info" => self.rpc_info(req.id, &req.arg, routing, addr),
            "rpc.listinfo" => self.rpc_listinfo(req.id, &req.arg, routing, addr),
            "dev.metadata" => self.rpc_metadata(req.id, &req.arg, routing, addr),
            "test.amplitude" => {
                let next = self.read_or_write_nonnegative_f64(
                    req.id,
                    &req.arg,
                    self.params.amplitude,
                    routing.clone(),
                    addr,
                )?;
                self.params.amplitude = next;
                Ok(())
            }
            "test.frequency" => {
                let next = self.read_or_write_nonnegative_f64(
                    req.id,
                    &req.arg,
                    self.params.frequency,
                    routing.clone(),
                    addr,
                )?;
                self.params.frequency = next;
                Ok(())
            }
            "test.noise" => {
                let next = self.read_or_write_nonnegative_f64(
                    req.id,
                    &req.arg,
                    self.params.noise,
                    routing.clone(),
                    addr,
                )?;
                self.params.noise = next;
                Ok(())
            }
            _ => self.send_rpc_error(req.id, proto::RpcErrorCode::NotFound, routing, addr),
        };

        result
    }

    fn rpc_read_string(
        &self,
        id: u16,
        value: &str,
        arg: &[u8],
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if !arg.is_empty() {
            return self.send_rpc_error(id, proto::RpcErrorCode::ReadOnly, routing, addr);
        }
        self.send_rpc_reply(id, value.as_bytes().to_vec(), routing, addr)
    }

    fn rpc_read_u32(
        &self,
        id: u16,
        value: u32,
        arg: &[u8],
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if !arg.is_empty() {
            return self.send_rpc_error(id, proto::RpcErrorCode::ReadOnly, routing, addr);
        }
        self.send_rpc_reply(id, value.to_le_bytes().to_vec(), routing, addr)
    }

    fn rpc_info(
        &self,
        id: u16,
        arg: &[u8],
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let name = match std::str::from_utf8(arg) {
            Ok(name) => name,
            Err(_) => {
                return self.send_rpc_error(id, proto::RpcErrorCode::InvalidArgs, routing, addr)
            }
        };
        let Some(spec) = self.rpcs.iter().find(|spec| spec.name == name) else {
            return self.send_rpc_error(id, proto::RpcErrorCode::NotFound, routing, addr);
        };
        self.send_rpc_reply(id, spec.meta.to_le_bytes().to_vec(), routing, addr)
    }

    fn rpc_listinfo(
        &self,
        id: u16,
        arg: &[u8],
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if arg.is_empty() {
            return self.send_rpc_reply(
                id,
                (self.rpcs.len() as u16).to_le_bytes().to_vec(),
                routing,
                addr,
            );
        }
        if arg.len() != 2 {
            return self.send_rpc_error(id, proto::RpcErrorCode::WrongSizeArgs, routing, addr);
        }

        let index = u16::from_le_bytes([arg[0], arg[1]]) as usize;
        let Some(spec) = self.rpcs.get(index) else {
            return self.send_rpc_error(id, proto::RpcErrorCode::InvalidArgs, routing, addr);
        };

        let mut reply = spec.meta.to_le_bytes().to_vec();
        reply.extend(spec.name.as_bytes());
        self.send_rpc_reply(id, reply, routing, addr)
    }

    fn rpc_metadata(
        &self,
        id: u16,
        arg: &[u8],
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let reply = if arg.is_empty() {
            self.all_metadata_reply()?
        } else if arg.len() % 3 == 0 {
            let mut reply = Vec::new();
            for req in arg.chunks_exact(3) {
                if self
                    .append_metadata_record(&mut reply, req[0], req[1], req[2])
                    .is_err()
                {
                    return self.send_rpc_error(
                        id,
                        proto::RpcErrorCode::InvalidArgs,
                        routing,
                        addr,
                    );
                }
            }
            reply
        } else {
            return self.send_rpc_error(id, proto::RpcErrorCode::WrongSizeArgs, routing, addr);
        };

        self.send_rpc_reply(id, reply, routing, addr)
    }

    fn read_or_write_nonnegative_f64(
        &self,
        id: u16,
        arg: &[u8],
        current: f64,
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<f64> {
        let value = match arg.len() {
            0 => current,
            8 => f64::from_le_bytes(arg.try_into().unwrap()),
            _ => {
                self.send_rpc_error(id, proto::RpcErrorCode::WrongSizeArgs, routing, addr)?;
                return Ok(current);
            }
        };

        if !value.is_finite() || value < 0.0 {
            self.send_rpc_error(id, proto::RpcErrorCode::InvalidArgs, routing, addr)?;
            return Ok(current);
        }

        self.send_rpc_reply(id, value.to_le_bytes().to_vec(), routing, addr)?;
        Ok(value)
    }

    fn send_periodic_packets(&mut self) -> io::Result<()> {
        let Some(client) = self.client else {
            return Ok(());
        };

        if self.last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
            self.send_packet(&self.heartbeat_packet(), client.addr)?;
            self.last_heartbeat = Instant::now();
        }

        self.send_due_samples(client.addr)
    }

    fn send_due_samples(&mut self, addr: SocketAddr) -> io::Result<()> {
        for _ in 0..4 {
            let due = self.due_samples();
            if due == 0 {
                break;
            }

            let first_sample_n = self.sample_number;
            let samples_left_in_segment = u64::from(self.segment_samples - self.sample_number);
            let batch_len = due.min(MAX_SAMPLES_PER_PACKET).min(samples_left_in_segment);
            self.send_sample_batch(batch_len, addr)?;

            if self.pending_segment_update && first_sample_n == 0 {
                self.send_packet(&self.segment_metadata().make_update(), addr)?;
                self.pending_segment_update = false;
            }
        }
        Ok(())
    }

    fn due_samples(&self) -> u64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let target = (elapsed * f64::from(self.sample_rate)).floor() as u64;
        target.saturating_sub(self.samples_generated)
    }

    fn send_sample_batch(&mut self, batch_len: u64, addr: SocketAddr) -> io::Result<()> {
        let first_sample_n = self.sample_number;
        let segment_id = self.segment_id;
        let mut data = Vec::with_capacity((batch_len as usize) * std::mem::size_of::<f64>());

        for _ in 0..batch_len {
            let t = self.samples_generated as f64 / f64::from(self.sample_rate);
            let signal =
                self.params.amplitude * (std::f64::consts::TAU * self.params.frequency * t).sin();
            let noise_sigma = self.params.noise * (f64::from(self.sample_rate) / 2.0).sqrt();
            let value = signal + noise_sigma * self.rng.next_gaussian();
            data.extend(value.to_le_bytes());
            self.advance_sample();
        }

        let packet = proto::Packet {
            payload: proto::Payload::StreamData(proto::StreamDataPayload {
                stream_id: STREAM_ID,
                first_sample_n,
                segment_id,
                data,
            }),
            routing: proto::DeviceRoute::root(),
            ttl: 0,
        };
        self.send_packet(&packet, addr)
    }

    fn advance_sample(&mut self) {
        self.samples_generated = self.samples_generated.wrapping_add(1);
        self.sample_number += 1;
        if self.sample_number >= self.segment_samples {
            self.sample_number = 0;
            self.segment_id = (self.segment_id + 1) % N_SEGMENTS;
            self.segment_start_time = self.segment_start_time.saturating_add(self.segment_seconds);
            self.pending_segment_update = true;
        }
    }

    fn send_initial_packets(&self, addr: SocketAddr) -> io::Result<()> {
        self.send_packet(&self.settings_packet(), addr)?;
        self.send_packet(&self.heartbeat_packet(), addr)?;
        self.send_packet(&self.device_metadata().make_update(), addr)?;
        self.send_packet(&self.stream_metadata().make_update(), addr)?;
        self.send_packet(&self.segment_metadata().make_update(), addr)?;
        self.send_packet(&self.column_metadata().make_update(), addr)?;
        Ok(())
    }

    fn send_rpc_reply(
        &self,
        id: u16,
        reply: Vec<u8>,
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.send_packet(
            &proto::Packet {
                payload: proto::Payload::RpcReply(proto::RpcReplyPayload { id, reply }),
                routing,
                ttl: 0,
            },
            addr,
        )
    }

    fn send_rpc_error(
        &self,
        id: u16,
        error: proto::RpcErrorCode,
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.send_packet(
            &proto::Packet {
                payload: proto::Payload::RpcError(proto::RpcErrorPayload {
                    id,
                    error,
                    extra: Vec::new(),
                }),
                routing,
                ttl: 0,
            },
            addr,
        )
    }

    fn send_packet(&self, packet: &proto::Packet, addr: SocketAddr) -> io::Result<()> {
        let raw = packet
            .serialize()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "packet too large"))?;
        self.socket.send_to(&raw, addr)?;
        Ok(())
    }

    fn all_metadata_reply(&self) -> io::Result<Vec<u8>> {
        let mut reply = Vec::new();
        self.append_metadata_record(&mut reply, u8::from(meta::MetadataType::Device), 0, 0)?;
        self.append_metadata_record(
            &mut reply,
            u8::from(meta::MetadataType::Stream),
            STREAM_ID,
            0,
        )?;
        self.append_metadata_record(
            &mut reply,
            u8::from(meta::MetadataType::Segment),
            STREAM_ID,
            self.segment_id,
        )?;
        self.append_metadata_record(
            &mut reply,
            u8::from(meta::MetadataType::Column),
            STREAM_ID,
            0,
        )?;
        Ok(reply)
    }

    fn append_metadata_record(
        &self,
        reply: &mut Vec<u8>,
        metadata_type: u8,
        stream_id: u8,
        index: u8,
    ) -> io::Result<()> {
        let (mtype, body) = match meta::MetadataType::from(metadata_type) {
            meta::MetadataType::Device => {
                let (fixed, varlen) = self
                    .device_metadata()
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "device metadata"))?;
                (meta::MetadataType::Device, join_metadata(fixed, varlen))
            }
            meta::MetadataType::Stream if stream_id == STREAM_ID => {
                let (fixed, varlen) = self
                    .stream_metadata()
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stream metadata"))?;
                (meta::MetadataType::Stream, join_metadata(fixed, varlen))
            }
            meta::MetadataType::Segment if stream_id == STREAM_ID => {
                let mut segment = self.segment_metadata();
                segment.segment_id = index;
                let delta_segments = u32::from((index + N_SEGMENTS - self.segment_id) % N_SEGMENTS);
                segment.start_time = self
                    .segment_start_time
                    .saturating_add(delta_segments.saturating_mul(self.segment_seconds));
                let (fixed, varlen) = segment
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment metadata"))?;
                (meta::MetadataType::Segment, join_metadata(fixed, varlen))
            }
            meta::MetadataType::Column if stream_id == STREAM_ID && index == 0 => {
                let (fixed, varlen) = self
                    .column_metadata()
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "column metadata"))?;
                (meta::MetadataType::Column, join_metadata(fixed, varlen))
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unknown metadata request",
                ))
            }
        };

        let len = u8::try_from(body.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "metadata too large"))?;
        reply.push(u8::from(mtype));
        reply.push(len);
        reply.extend(body);
        Ok(())
    }

    fn device_metadata(&self) -> meta::DeviceMetadata {
        meta::DeviceMetadata {
            serial_number: DEVICE_SERIAL.to_string(),
            firmware_hash: DEVICE_FIRMWARE.to_string(),
            n_streams: 1,
            session_id: self.session_id,
            name: DEVICE_NAME.to_string(),
        }
    }

    fn stream_metadata(&self) -> meta::StreamMetadata {
        meta::StreamMetadata {
            stream_id: STREAM_ID,
            name: "sine".to_string(),
            n_columns: 1,
            n_segments: N_SEGMENTS as usize,
            sample_size: std::mem::size_of::<f64>(),
            buf_samples: self.sample_rate as usize,
        }
    }

    fn segment_metadata(&self) -> meta::SegmentMetadata {
        meta::SegmentMetadata {
            stream_id: STREAM_ID,
            segment_id: self.segment_id,
            flags: 0x01 | 0x02,
            time_ref_epoch: meta::MetadataEpoch::Unix,
            time_ref_serial: DEVICE_SERIAL.to_string(),
            time_ref_session_id: self.session_id,
            start_time: self.segment_start_time,
            sampling_rate: self.sample_rate,
            decimation: 1,
            filter_cutoff: self.sample_rate as f32 / 2.0,
            filter_type: meta::MetadataFilter::Unfiltered,
        }
    }

    fn column_metadata(&self) -> meta::ColumnMetadata {
        meta::ColumnMetadata {
            stream_id: STREAM_ID,
            index: 0,
            data_type: proto::DataType::Float64,
            name: "value".to_string(),
            units: "V".to_string(),
            description: "Noisy sine wave".to_string(),
        }
    }

    fn heartbeat_packet(&self) -> proto::Packet {
        proto::Packet {
            payload: proto::Payload::Heartbeat(proto::HeartbeatPayload::Session(self.session_id)),
            routing: proto::DeviceRoute::root(),
            ttl: 0,
        }
    }

    fn settings_packet(&self) -> proto::Packet {
        proto::Packet {
            payload: proto::Payload::Settings(proto::SettingsPayload::RpcHash(RPC_HASH)),
            routing: proto::DeviceRoute::root(),
            ttl: 0,
        }
    }
}

fn join_metadata(mut fixed: Vec<u8>, varlen: Vec<u8>) -> Vec<u8> {
    fixed.extend(varlen);
    fixed
}

fn unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
}

fn unix_time_secs(now: Duration) -> u32 {
    u32::try_from(now.as_secs()).unwrap_or(u32::MAX)
}

pub fn run_test(cli: TestCli) -> eyre::Result<()> {
    let mut device = TestDevice::new(cli)?;
    device.run()?;
    Ok(())
}

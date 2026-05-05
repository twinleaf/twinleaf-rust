//! tio test
//!
//! Simulates a small Twinleaf device that publishes a noisy sine wave on stream 1.

use crate::TestCli;
use ratatui::crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::tio::proto;
use twinleaf::tio::proto::meta;

macro_rules! terminal_println {
    ($($arg:tt)*) => {
        terminal_print_line(format_args!($($arg)*))
    };
}

macro_rules! terminal_eprintln {
    ($($arg:tt)*) => {
        terminal_error_line(format_args!($($arg)*))
    };
}

const SINE_STREAM_ID: u8 = 1;
const STATUS_STREAM_ID: u8 = 2;
const AUX_STREAM_ID: u8 = 3;
const N_SEGMENTS: u8 = 16;
const RPC_HASH: u32 = 0x7465_7375;
const DEVICE_NAME: &str = "tio-test";
const DEVICE_SERIAL: &str = "SIM0001";
const DEVICE_FIRMWARE: &str = "twinleaf-rust-test";
const SIGNAL_LEVEL: u8 = 234;
const AUX_SAMPLE_RATE: u32 = 25;
const AUX_WAVE_FREQUENCY: f64 = 0.25;
const SAMPLE_DROP_INTERVAL_SECONDS: f64 = 60.0;
const SAMPLE_DROP_JITTER_SECONDS: f64 = 30.0;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_SAMPLE_NUMBER: u32 = 0x00ff_ffff;
const STREAM_DATA_HEADER_BYTES: usize = 4;
const SINE_SAMPLE_BYTES: usize = std::mem::size_of::<f64>() * 2;
const STATUS_SAMPLE_BYTES: usize = 2;
const AUX_SAMPLE_BYTES: usize = std::mem::size_of::<f64>() * 2;

const META_F64_RW: u16 = 0x0100 | 0x0200 | (8 << 4) | 2;
const META_U8_RW: u16 = 0x0100 | 0x0200 | (1 << 4);
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

struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> io::Result<Self> {
        enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

fn terminal_print_line(args: std::fmt::Arguments<'_>) {
    let mut stdout = io::stdout().lock();
    let _ = write!(stdout, "{args}\r\n");
    let _ = stdout.flush();
}

fn terminal_error_line(args: std::fmt::Arguments<'_>) {
    let mut stderr = io::stderr().lock();
    let _ = write!(stderr, "{args}\r\n");
    let _ = stderr.flush();
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
    initial_params: SineParams,
    params: SineParams,
    initial_status: u8,
    status: u8,
    sample_rate: u32,
    segment_seconds: u32,
    segment_samples: u32,
    max_samples_per_packet: u64,
    aux_segment_samples: u32,
    aux_max_samples_per_packet: u64,
    session_id: u32,
    started_at: Instant,
    start_time: u32,
    samples_generated: u64,
    sample_number: u32,
    segment_id: u8,
    segment_start_time: u32,
    pending_segment_update: bool,
    next_drop_sample: u64,
    aux_samples_generated: u64,
    aux_sample_number: u32,
    aux_segment_id: u8,
    aux_segment_start_time: u32,
    aux_pending_segment_update: bool,
    next_aux_drop_sample: u64,
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
        let aux_segment_samples = AUX_SAMPLE_RATE
            .checked_mul(cli.segment_seconds)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "aux segment too long"))?;
        if aux_segment_samples > MAX_SAMPLE_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aux segment contains too many samples for TIO sample numbering",
            ));
        }
        let max_samples_per_packet = max_stream_samples_per_packet(SINE_SAMPLE_BYTES)
            .min(max_stream_samples_per_packet(STATUS_SAMPLE_BYTES));
        let aux_max_samples_per_packet = max_stream_samples_per_packet(AUX_SAMPLE_BYTES);
        if max_samples_per_packet == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream sample is too large for a TIO packet",
            ));
        }
        if aux_max_samples_per_packet == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aux stream sample is too large for a TIO packet",
            ));
        }
        let mut rng = GaussianRng::new(seed | 1);
        let next_drop_sample = next_drop_sample_after(&mut rng, 0, cli.samplerate);
        let next_aux_drop_sample = next_drop_sample_after(&mut rng, 0, AUX_SAMPLE_RATE);

        let initial_params = SineParams {
            amplitude: cli.amplitude,
            frequency: cli.frequency,
            noise: cli.noise,
        };
        let initial_status = 0;

        Ok(Self {
            socket,
            client: None,
            initial_params,
            params: initial_params,
            initial_status,
            status: initial_status,
            sample_rate: cli.samplerate,
            segment_seconds: cli.segment_seconds,
            segment_samples,
            max_samples_per_packet,
            aux_segment_samples,
            aux_max_samples_per_packet,
            session_id,
            started_at: Instant::now(),
            start_time,
            samples_generated: 0,
            sample_number: 0,
            segment_id: 0,
            segment_start_time: start_time,
            pending_segment_update: false,
            next_drop_sample,
            aux_samples_generated: 0,
            aux_sample_number: 0,
            aux_segment_id: 0,
            aux_segment_start_time: start_time,
            aux_pending_segment_update: false,
            next_aux_drop_sample,
            last_heartbeat: Instant::now(),
            rng,
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
                RpcSpec {
                    name: "test.status",
                    meta: META_U8_RW,
                },
            ],
        })
    }

    fn run(&mut self) -> io::Result<()> {
        let raw_mode = match RawModeGuard::enable() {
            Ok(guard) => Some(guard),
            Err(err) => {
                terminal_eprintln!("keyboard shortcuts disabled: {err}");
                None
            }
        };

        terminal_println!(
            "tio test listening on udp://0.0.0.0:{}",
            self.socket.local_addr()?.port()
        );
        terminal_println!(
            "  stream 1: 2 waveform channels, amplitude={} V frequency={} Hz noise={} V/sqrt(Hz) samplerate={} Hz segment={} s",
            self.params.amplitude,
            self.params.frequency,
            self.params.noise,
            self.sample_rate,
            self.segment_seconds
        );
        terminal_println!(
            "  stream 2: status={} signal_level={}",
            self.status,
            SIGNAL_LEVEL
        );
        terminal_println!(
            "  stream 3: aux triangle/sawtooth at {} Hz sampled at {} Hz",
            AUX_WAVE_FREQUENCY,
            AUX_SAMPLE_RATE
        );
        terminal_println!(
            "  randomly dropping one sample from each sample clock about once per minute"
        );
        if raw_mode.is_some() {
            terminal_println!("  press d to drop one sample now, r to reboot, Ctrl-C to quit");
        }
        terminal_println!(
            "  connect with: tio proxy udp4://127.0.0.1:{}",
            self.socket.local_addr()?.port()
        );

        loop {
            if raw_mode.is_some() && !self.handle_keyboard()? {
                terminal_println!("stopping tio test");
                return Ok(());
            }
            self.receive_packets()?;
            self.expire_client();
            self.send_periodic_packets()?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn handle_keyboard(&mut self) -> io::Result<bool> {
        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('d') => self.drop_samples_now()?,
                    KeyCode::Char('r') => self.reboot()?,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(false);
                    }
                    _ => {}
                }
            }
        }
        Ok(true)
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
                            terminal_eprintln!(
                                "Ignoring UDP datagram with trailing bytes from {addr}"
                            );
                        }
                        Err(err) => {
                            terminal_eprintln!("Ignoring malformed packet from {addr}: {err:?}");
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
                terminal_println!("client connected: {addr}");
                self.send_initial_packets(addr)?;
                Ok(true)
            }
        }
    }

    fn expire_client(&mut self) {
        if let Some(client) = self.client {
            if Instant::now().duration_since(client.last_rx) > CLIENT_TIMEOUT {
                terminal_println!("client disconnected: {}", client.addr);
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
        let next_drop_sample = self.next_drop_sample_after(0, self.sample_rate);
        self.next_drop_sample = next_drop_sample;
        self.aux_samples_generated = 0;
        self.aux_sample_number = 0;
        self.aux_segment_id = 0;
        self.aux_segment_start_time = self.start_time;
        self.aux_pending_segment_update = false;
        let next_aux_drop_sample = self.next_drop_sample_after(0, AUX_SAMPLE_RATE);
        self.next_aux_drop_sample = next_aux_drop_sample;
        self.last_heartbeat = Instant::now()
            .checked_sub(HEARTBEAT_INTERVAL)
            .unwrap_or_else(Instant::now);
    }

    fn reboot(&mut self) -> io::Result<()> {
        self.session_id = self.next_session_id();
        self.params = self.initial_params;
        self.status = self.initial_status;
        self.reset_run();
        terminal_println!("rebooted test device; new session id {}", self.session_id);

        if let Some(client) = self.client {
            self.send_initial_packets(client.addr)?;
        }
        Ok(())
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
            "test.status" => {
                let next =
                    self.read_or_write_u8(req.id, &req.arg, self.status, routing.clone(), addr)?;
                self.status = next;
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

    fn read_or_write_u8(
        &self,
        id: u16,
        arg: &[u8],
        current: u8,
        routing: proto::DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<u8> {
        let value = match arg.len() {
            0 => current,
            1 => arg[0],
            _ => {
                self.send_rpc_error(id, proto::RpcErrorCode::WrongSizeArgs, routing, addr)?;
                return Ok(current);
            }
        };

        self.send_rpc_reply(id, vec![value], routing, addr)?;
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

        self.send_due_samples(client.addr)?;
        self.send_due_aux_samples(client.addr)
    }

    fn send_due_samples(&mut self, addr: SocketAddr) -> io::Result<()> {
        for _ in 0..4 {
            let due = self.due_samples();
            if due == 0 {
                break;
            }

            let first_sample_n = self.sample_number;
            let samples_until_drop = self.next_drop_sample.saturating_sub(self.samples_generated);
            if samples_until_drop == 0 {
                self.drop_sample();
                self.send_sample_segment_updates_if_needed(first_sample_n, addr)?;
                continue;
            }

            let samples_left_in_segment = u64::from(self.segment_samples - self.sample_number);
            let batch_len = due
                .min(self.max_samples_per_packet)
                .min(samples_left_in_segment)
                .min(samples_until_drop);
            self.send_sample_batches(batch_len, addr)?;
            self.send_sample_segment_updates_if_needed(first_sample_n, addr)?;
        }
        Ok(())
    }

    fn due_samples(&self) -> u64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let target = (elapsed * f64::from(self.sample_rate)).floor() as u64;
        target.saturating_sub(self.samples_generated)
    }

    fn due_aux_samples(&self) -> u64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let target = (elapsed * f64::from(AUX_SAMPLE_RATE)).floor() as u64;
        target.saturating_sub(self.aux_samples_generated)
    }

    fn send_sample_batches(&mut self, batch_len: u64, addr: SocketAddr) -> io::Result<()> {
        let first_sample_n = self.sample_number;
        let segment_id = self.segment_id;
        let mut waveform_data = Vec::with_capacity((batch_len as usize) * SINE_SAMPLE_BYTES);
        let mut status_data = Vec::with_capacity((batch_len as usize) * STATUS_SAMPLE_BYTES);
        let noise_sigma = self.params.noise * (f64::from(self.sample_rate) / 2.0).sqrt();

        for offset in 0..batch_len {
            let t = (self.samples_generated + offset) as f64 / f64::from(self.sample_rate);
            let phase = std::f64::consts::TAU * self.params.frequency * t;
            let ch1 = self.params.amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
            let ch2 = self.params.amplitude * phase.cos() + noise_sigma * self.rng.next_gaussian();
            waveform_data.extend(ch1.to_le_bytes());
            waveform_data.extend(ch2.to_le_bytes());
            status_data.push(self.status);
            status_data.push(SIGNAL_LEVEL);
        }

        self.send_stream_packet(
            SINE_STREAM_ID,
            first_sample_n,
            segment_id,
            waveform_data,
            addr,
        )?;
        self.send_stream_packet(
            STATUS_STREAM_ID,
            first_sample_n,
            segment_id,
            status_data,
            addr,
        )?;

        for _ in 0..batch_len {
            self.advance_sample();
        }

        Ok(())
    }

    fn send_due_aux_samples(&mut self, addr: SocketAddr) -> io::Result<()> {
        for _ in 0..4 {
            let due = self.due_aux_samples();
            if due == 0 {
                break;
            }

            let first_sample_n = self.aux_sample_number;
            let samples_until_drop = self
                .next_aux_drop_sample
                .saturating_sub(self.aux_samples_generated);
            if samples_until_drop == 0 {
                self.drop_aux_sample();
                self.send_aux_segment_update_if_needed(first_sample_n, addr)?;
                continue;
            }

            let samples_left_in_segment =
                u64::from(self.aux_segment_samples - self.aux_sample_number);
            let batch_len = due
                .min(self.aux_max_samples_per_packet)
                .min(samples_left_in_segment)
                .min(samples_until_drop);
            self.send_aux_sample_batch(batch_len, addr)?;
            self.send_aux_segment_update_if_needed(first_sample_n, addr)?;
        }
        Ok(())
    }

    fn send_aux_sample_batch(&mut self, batch_len: u64, addr: SocketAddr) -> io::Result<()> {
        let first_sample_n = self.aux_sample_number;
        let segment_id = self.aux_segment_id;
        let mut data = Vec::with_capacity((batch_len as usize) * AUX_SAMPLE_BYTES);

        for offset in 0..batch_len {
            let t = (self.aux_samples_generated + offset) as f64 / f64::from(AUX_SAMPLE_RATE);
            let phase = (AUX_WAVE_FREQUENCY * t).fract();
            let triangle = 1.0 - 4.0 * (phase - 0.5).abs();
            let sawtooth = 2.0 * phase - 1.0;
            data.extend(triangle.to_le_bytes());
            data.extend(sawtooth.to_le_bytes());
        }

        self.send_stream_packet(AUX_STREAM_ID, first_sample_n, segment_id, data, addr)?;

        for _ in 0..batch_len {
            self.advance_aux_sample();
        }

        Ok(())
    }

    fn send_stream_packet(
        &self,
        stream_id: u8,
        first_sample_n: u32,
        segment_id: u8,
        data: Vec<u8>,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.send_packet(
            &proto::Packet {
                payload: proto::Payload::StreamData(proto::StreamDataPayload {
                    stream_id,
                    first_sample_n,
                    segment_id,
                    data,
                }),
                routing: proto::DeviceRoute::root(),
                ttl: 0,
            },
            addr,
        )
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

    fn advance_aux_sample(&mut self) {
        self.aux_samples_generated = self.aux_samples_generated.wrapping_add(1);
        self.aux_sample_number += 1;
        if self.aux_sample_number >= self.aux_segment_samples {
            self.aux_sample_number = 0;
            self.aux_segment_id = (self.aux_segment_id + 1) % N_SEGMENTS;
            self.aux_segment_start_time = self
                .aux_segment_start_time
                .saturating_add(self.segment_seconds);
            self.aux_pending_segment_update = true;
        }
    }

    fn drop_sample(&mut self) {
        terminal_println!(
            "dropped sample {} from streams {}/{}",
            self.samples_generated,
            SINE_STREAM_ID,
            STATUS_STREAM_ID
        );
        self.advance_sample();
        self.next_drop_sample =
            self.next_drop_sample_after(self.samples_generated, self.sample_rate);
    }

    fn drop_aux_sample(&mut self) {
        terminal_println!(
            "dropped sample {} from stream {}",
            self.aux_samples_generated,
            AUX_STREAM_ID
        );
        self.advance_aux_sample();
        self.next_aux_drop_sample =
            self.next_drop_sample_after(self.aux_samples_generated, AUX_SAMPLE_RATE);
    }

    fn drop_samples_now(&mut self) -> io::Result<()> {
        let addr = self.client.map(|client| client.addr);
        let first_sample_n = self.sample_number;
        self.drop_sample();
        if let Some(addr) = addr {
            self.send_sample_segment_updates_if_needed(first_sample_n, addr)?;
        }

        let first_aux_sample_n = self.aux_sample_number;
        self.drop_aux_sample();
        if let Some(addr) = addr {
            self.send_aux_segment_update_if_needed(first_aux_sample_n, addr)?;
        }

        Ok(())
    }

    fn next_drop_sample_after(&mut self, current_sample: u64, sample_rate: u32) -> u64 {
        next_drop_sample_after(&mut self.rng, current_sample, sample_rate)
    }

    fn next_session_id(&mut self) -> u32 {
        let mut session_id = (self.rng.next_u64() as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        if session_id == self.session_id {
            session_id = session_id.wrapping_add(1);
        }
        session_id
    }

    fn send_sample_segment_updates_if_needed(
        &mut self,
        first_sample_n: u32,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if self.pending_segment_update && first_sample_n == 0 {
            for stream_id in [SINE_STREAM_ID, STATUS_STREAM_ID] {
                self.send_packet(&self.segment_metadata(stream_id).make_update(), addr)?;
            }
            self.pending_segment_update = false;
        }
        Ok(())
    }

    fn send_aux_segment_update_if_needed(
        &mut self,
        first_sample_n: u32,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if self.aux_pending_segment_update && first_sample_n == 0 {
            self.send_packet(&self.segment_metadata(AUX_STREAM_ID).make_update(), addr)?;
            self.aux_pending_segment_update = false;
        }
        Ok(())
    }

    fn send_initial_packets(&self, addr: SocketAddr) -> io::Result<()> {
        self.send_packet(&self.settings_packet(), addr)?;
        self.send_packet(&self.heartbeat_packet(), addr)?;
        self.send_packet(&self.device_metadata().make_update(), addr)?;
        for stream_id in Self::stream_ids() {
            self.send_packet(
                &self
                    .stream_metadata(stream_id)
                    .expect("known stream")
                    .make_update(),
                addr,
            )?;
            self.send_packet(&self.segment_metadata(stream_id).make_update(), addr)?;
            for column_index in 0..Self::column_count(stream_id).expect("known stream") {
                self.send_packet(
                    &self
                        .column_metadata(stream_id, column_index)
                        .expect("known column")
                        .make_update(),
                    addr,
                )?;
            }
        }
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
        let raw = packet.serialize().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("packet too large or invalid: {}", describe_packet(packet)),
            )
        })?;
        self.socket.send_to(&raw, addr)?;
        Ok(())
    }

    fn all_metadata_reply(&self) -> io::Result<Vec<u8>> {
        let mut reply = Vec::new();
        self.append_metadata_record(&mut reply, u8::from(meta::MetadataType::Device), 0, 0)?;
        for stream_id in Self::stream_ids() {
            self.append_metadata_record(
                &mut reply,
                u8::from(meta::MetadataType::Stream),
                stream_id,
                0,
            )?;
            self.append_metadata_record(
                &mut reply,
                u8::from(meta::MetadataType::Segment),
                stream_id,
                self.segment_id,
            )?;
            for column_index in 0..Self::column_count(stream_id).expect("known stream") {
                self.append_metadata_record(
                    &mut reply,
                    u8::from(meta::MetadataType::Column),
                    stream_id,
                    column_index,
                )?;
            }
        }
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
            meta::MetadataType::Stream => {
                let Some(stream) = self.stream_metadata(stream_id) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unknown stream metadata",
                    ));
                };
                let (fixed, varlen) = stream
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stream metadata"))?;
                (meta::MetadataType::Stream, join_metadata(fixed, varlen))
            }
            meta::MetadataType::Segment if Self::is_known_stream(stream_id) => {
                let mut segment = self.segment_metadata(stream_id);
                segment.segment_id = index;
                let current_segment_id = if stream_id == AUX_STREAM_ID {
                    self.aux_segment_id
                } else {
                    self.segment_id
                };
                let current_start_time = if stream_id == AUX_STREAM_ID {
                    self.aux_segment_start_time
                } else {
                    self.segment_start_time
                };
                let delta_segments =
                    u32::from((index + N_SEGMENTS - current_segment_id) % N_SEGMENTS);
                segment.start_time = current_start_time
                    .saturating_add(delta_segments.saturating_mul(self.segment_seconds));
                let (fixed, varlen) = segment
                    .serialize(&[], &[])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment metadata"))?;
                (meta::MetadataType::Segment, join_metadata(fixed, varlen))
            }
            meta::MetadataType::Column => {
                let Some(column) = self.column_metadata(stream_id, index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unknown column metadata",
                    ));
                };
                let (fixed, varlen) = column
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
            n_streams: 3,
            session_id: self.session_id,
            name: DEVICE_NAME.to_string(),
        }
    }

    fn stream_metadata(&self, stream_id: u8) -> Option<meta::StreamMetadata> {
        let stream = match stream_id {
            SINE_STREAM_ID => meta::StreamMetadata {
                stream_id: SINE_STREAM_ID,
                name: "sine".to_string(),
                n_columns: 2,
                n_segments: N_SEGMENTS as usize,
                sample_size: SINE_SAMPLE_BYTES,
                buf_samples: self.sample_rate as usize,
            },
            STATUS_STREAM_ID => meta::StreamMetadata {
                stream_id: STATUS_STREAM_ID,
                name: "status".to_string(),
                n_columns: 2,
                n_segments: N_SEGMENTS as usize,
                sample_size: STATUS_SAMPLE_BYTES,
                buf_samples: self.sample_rate as usize,
            },
            AUX_STREAM_ID => meta::StreamMetadata {
                stream_id: AUX_STREAM_ID,
                name: "aux".to_string(),
                n_columns: 2,
                n_segments: N_SEGMENTS as usize,
                sample_size: AUX_SAMPLE_BYTES,
                buf_samples: AUX_SAMPLE_RATE as usize,
            },
            _ => return None,
        };
        Some(stream)
    }

    fn segment_metadata(&self, stream_id: u8) -> meta::SegmentMetadata {
        let (segment_id, start_time, sampling_rate) = match stream_id {
            AUX_STREAM_ID => (
                self.aux_segment_id,
                self.aux_segment_start_time,
                AUX_SAMPLE_RATE,
            ),
            _ => (self.segment_id, self.segment_start_time, self.sample_rate),
        };

        meta::SegmentMetadata {
            stream_id,
            segment_id,
            flags: 0x01 | 0x02,
            time_ref_epoch: meta::MetadataEpoch::Unix,
            time_ref_serial: DEVICE_SERIAL.to_string(),
            time_ref_session_id: self.session_id,
            start_time,
            sampling_rate,
            decimation: 1,
            filter_cutoff: sampling_rate as f32 / 2.0,
            filter_type: meta::MetadataFilter::Unfiltered,
        }
    }

    fn column_metadata(&self, stream_id: u8, index: u8) -> Option<meta::ColumnMetadata> {
        let column = match (stream_id, index) {
            (SINE_STREAM_ID, 0) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::Float64,
                name: "sine".to_string(),
                units: "V".to_string(),
                description: "Noisy sine wave".to_string(),
            },
            (SINE_STREAM_ID, 1) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::Float64,
                name: "cosine".to_string(),
                units: "V".to_string(),
                description: "Noisy quadrature wave".to_string(),
            },
            (STATUS_STREAM_ID, 0) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::UInt8,
                name: "status".to_string(),
                units: "".to_string(),
                description: "Mirrors the test.status RPC".to_string(),
            },
            (STATUS_STREAM_ID, 1) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::UInt8,
                name: "signal_level".to_string(),
                units: "".to_string(),
                description: "Fixed simulated signal level".to_string(),
            },
            (AUX_STREAM_ID, 0) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::Float64,
                name: "triangle".to_string(),
                units: "arb".to_string(),
                description: "Triangle wave".to_string(),
            },
            (AUX_STREAM_ID, 1) => meta::ColumnMetadata {
                stream_id,
                index: index.into(),
                data_type: proto::DataType::Float64,
                name: "sawtooth".to_string(),
                units: "arb".to_string(),
                description: "Sawtooth wave".to_string(),
            },
            _ => return None,
        };
        Some(column)
    }

    fn is_known_stream(stream_id: u8) -> bool {
        matches!(stream_id, SINE_STREAM_ID | STATUS_STREAM_ID | AUX_STREAM_ID)
    }

    fn column_count(stream_id: u8) -> Option<u8> {
        match stream_id {
            SINE_STREAM_ID | STATUS_STREAM_ID | AUX_STREAM_ID => Some(2),
            _ => None,
        }
    }

    fn stream_ids() -> [u8; 3] {
        [SINE_STREAM_ID, STATUS_STREAM_ID, AUX_STREAM_ID]
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

fn stream_data_max_data_bytes() -> usize {
    proto::TIO_PACKET_MAX_TOTAL_SIZE
        .saturating_sub(proto::TIO_PACKET_HEADER_SIZE)
        .saturating_sub(proto::TIO_PACKET_MAX_ROUTING_SIZE)
        .saturating_sub(STREAM_DATA_HEADER_BYTES)
}

fn max_stream_samples_per_packet(sample_bytes: usize) -> u64 {
    if sample_bytes == 0 {
        return 0;
    }
    (stream_data_max_data_bytes() / sample_bytes) as u64
}

fn next_drop_sample_after(rng: &mut GaussianRng, current_sample: u64, sample_rate: u32) -> u64 {
    let min_seconds = SAMPLE_DROP_INTERVAL_SECONDS - SAMPLE_DROP_JITTER_SECONDS;
    let seconds = min_seconds + rng.next_unit() * SAMPLE_DROP_JITTER_SECONDS * 2.0;
    let interval = (seconds * f64::from(sample_rate)).round().max(1.0) as u64;
    current_sample.saturating_add(interval)
}

fn describe_packet(packet: &proto::Packet) -> String {
    match &packet.payload {
        proto::Payload::StreamData(data) => format!(
            "stream data stream_id={} segment_id={} first_sample_n={} data_bytes={} max_data_bytes={}",
            data.stream_id,
            data.segment_id,
            data.first_sample_n,
            data.data.len(),
            stream_data_max_data_bytes()
        ),
        proto::Payload::Metadata(metadata) => format!("metadata {:?}", metadata.content),
        proto::Payload::RpcReply(reply) => {
            format!("rpc reply id={} reply_bytes={}", reply.id, reply.reply.len())
        }
        proto::Payload::RpcError(error) => {
            format!("rpc error id={} error={:?}", error.id, error.error)
        }
        proto::Payload::RpcRequest(request) => format!(
            "rpc request id={} method={:?} arg_bytes={}",
            request.id,
            request.method,
            request.arg.len()
        ),
        other => format!("{other:?}"),
    }
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

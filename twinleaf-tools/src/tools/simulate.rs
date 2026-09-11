//! tio simulate
//!
//! Simulates a small Twinleaf device that publishes a noisy sine wave on stream 1.

use crate::SimulateCli;
use ratatui::crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
// Incoming packets are still parsed with the host's owned model; everything
// this device *sends* is written by the wire crate, as firmware would.
use twinleaf::proto;
use twinleaf::proto::capture::{CaptureMetadata, METADATA_VERSION};
use twinleaf::proto::data::CURRENT_SEGMENT;
use twinleaf::proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf::proto::{data, heartbeat, log, rpc, settings, sync};
use twinleaf::proto::{
    ColumnId, DeviceRoute, RpcRequestId, SampleNumber, SegmentId, SessionId, StreamId,
};
use twinleaf::tio::packet;
use twinleaf_device::capture::{self, Capture, Selector};
use twinleaf_device::metadata::{self, Records};
use twinleaf_device::rpc::{self as table, Access, RpcSpec, Value};

pub fn run_simulate(cli: SimulateCli) -> eyre::Result<()> {
    let mut device = TestDevice::new(cli)?;
    device.run()?;
    Ok(())
}

// In raw mode, '\n' does not reliably return the cursor to column 0.
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
const STREAM_IDS: [u8; 3] = [SINE_STREAM_ID, STATUS_STREAM_ID, AUX_STREAM_ID];
const N_SEGMENTS: u8 = 16;
const DEVICE_NAME: &str = "tio-test";
// The simulator is a development/test device, so its build version is marked
// `DEV`. Format: `{vendor} {name} {revision} ({serial}) [{date}/{build}]`.
const DEVICE_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000001-DEV]";
const DEVICE_SERIAL: &str = "SIM0001";
const DEVICE_FIRMWARE: &str = "twinleaf-rust-test";
const SIGNAL_LEVEL: u8 = 234;
const AUX_SAMPLE_RATE: u32 = 25;
const AUX_WAVE_FREQUENCY: f64 = 0.25;
const SAMPLE_DROP_INTERVAL_SECONDS: f64 = 60.0;
const SAMPLE_DROP_JITTER_SECONDS: f64 = 30.0;
/// "Never" sample-drop deadline used when random drops are disabled (`--no-drop`).
/// `samples_generated` would need to run for billions of years to reach it.
const NO_DROP_SENTINEL: u64 = u64::MAX;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const LOG_MESSAGE_MIN_INTERVAL: Duration = Duration::from_millis(1500);
const LOG_MESSAGE_JITTER: Duration = Duration::from_millis(4000);
const CAPTURE_TRIGGER_DELAY: Duration = Duration::from_millis(500);
const CAPTURE_DEFAULT_BLOCK_SIZE: u16 = 256;
const CAPTURE_SAMPLE_COUNT_MIN: usize = 800;
const CAPTURE_SAMPLE_COUNT_MAX: usize = 1200;
const CAPTURE_SAMPLE_BYTES: usize = std::mem::size_of::<f32>();
const CAPTURE_Y_CALIBRATION: f32 = 1.0;
const CAPTURE_NAME: &str = "Test Signal";
const CAPTURE_UNITS: &str = "V";
const CAPTURE_X_NAME: &str = "Time";
const CAPTURE_X_UNITS: &str = "s";
const SINE_SAMPLE_BYTES: usize = std::mem::size_of::<f64>() * 2;
const STATUS_SAMPLE_BYTES: usize = 2;
const AUX_SAMPLE_BYTES: usize = std::mem::size_of::<f64>() * 2;

#[derive(Clone, Copy)]
struct SineParams {
    amplitude: f64,
    frequency: f64,
    noise: f64,
}

#[derive(Clone, Copy)]
struct Client {
    addr: SocketAddr,
    last_rx: Instant,
}

#[derive(Clone, Copy)]
struct CaptureInfo {
    length: u32,
    y_calibration: f32,
    x_offset: f32,
    x_stride: f32,
}

impl Default for CaptureInfo {
    fn default() -> Self {
        Self {
            length: 0,
            y_calibration: CAPTURE_Y_CALIBRATION,
            x_offset: 0.0,
            x_stride: 0.0,
        }
    }
}

struct CapturingCapture {
    ready_at: Instant,
    data: Vec<u8>,
    info: CaptureInfo,
}

struct CaptureBuffer {
    data: Vec<u8>,
    block_size: u16,
    capturing: Option<CapturingCapture>,
    info: CaptureInfo,
}

impl CaptureBuffer {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            block_size: CAPTURE_DEFAULT_BLOCK_SIZE,
            capturing: None,
            info: CaptureInfo::default(),
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.capturing = None;
        self.block_size = CAPTURE_DEFAULT_BLOCK_SIZE;
        self.info = CaptureInfo::default();
    }

    fn begin_capture(&mut self, data: Vec<u8>, info: CaptureInfo, ready_at: Instant) {
        self.capturing = Some(CapturingCapture {
            ready_at,
            data,
            info,
        });
    }

    fn update(&mut self, now: Instant) {
        let Some(capturing) = self.capturing.as_ref() else {
            return;
        };
        if now < capturing.ready_at {
            return;
        }

        let capturing = self.capturing.take().expect("capturing checked above");
        self.data = capturing.data;
        self.info = capturing.info;
    }

    fn locked(&self) -> bool {
        self.capturing.is_some()
    }

    fn status(&self) -> capture::Status {
        if self.capturing.is_some() {
            capture::Status::Capturing
        } else if self.data.is_empty() {
            capture::Status::Idle
        } else {
            capture::Status::Done
        }
    }

    fn info(&self) -> CaptureInfo {
        self.capturing
            .as_ref()
            .map(|capturing| capturing.info)
            .unwrap_or(self.info)
    }

    fn export_size(&self) -> usize {
        self.capturing
            .as_ref()
            .map(|capturing| capturing.data.len())
            .unwrap_or(self.data.len())
    }

    fn view(&self) -> Capture<'_> {
        let info = self.info();
        Capture {
            status: self.status(),
            data: &self.data,
            metadata: CaptureMetadata {
                version: METADATA_VERSION,
                data_type: data::DataType::F32,
                data_size: u32::try_from(self.export_size()).unwrap_or(u32::MAX),
                block_size: self.block_size,
                length: info.length,
                y_calibration: info.y_calibration,
                x_offset: info.x_offset,
                x_stride: info.x_stride,
                name: CAPTURE_NAME,
                units: CAPTURE_UNITS,
                x_name: CAPTURE_X_NAME,
                x_units: CAPTURE_X_UNITS,
            },
        }
    }
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
    initial_enable: u8,
    enable: u8,
    // Mutable device description (dev.desc). A firmware upgrade rewrites it so
    // a post-upgrade re-read shows a different build.
    desc: String,
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
    no_drop: bool,
    aux_samples_generated: u64,
    aux_sample_number: u32,
    aux_segment_id: u8,
    aux_segment_start_time: u32,
    aux_pending_segment_update: bool,
    next_aux_drop_sample: u64,
    last_heartbeat: Instant,
    next_log_message_at: Instant,
    next_log_level: usize,
    capture: CaptureBuffer,
    rng: GaussianRng,
    rpcs: Vec<RpcSpec>,
    rpc_hash: u32,
}

impl TestDevice {
    fn new(cli: SimulateCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;

        let now = unix_duration();
        let start_time = unix_time_secs(now);
        let seed = now.as_nanos() as u64 ^ u64::from(cli.port).rotate_left(32);
        let session_id = (seed as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        if u16::try_from(cli.samplerate).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sample rate too high for a stream record",
            ));
        }
        let segment_samples = cli
            .samplerate
            .checked_mul(cli.segment_seconds)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "segment too long"))?;
        if segment_samples > data::MAX_SAMPLE_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment contains too many samples for TIO sample numbering",
            ));
        }
        let aux_segment_samples = AUX_SAMPLE_RATE
            .checked_mul(cli.segment_seconds)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "aux segment too long"))?;
        if aux_segment_samples > data::MAX_SAMPLE_NUMBER {
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
        let no_drop = cli.no_drop;
        let next_drop_sample = if no_drop {
            NO_DROP_SENTINEL
        } else {
            next_drop_sample_after(&mut rng, 0, cli.samplerate)
        };
        let next_aux_drop_sample = if no_drop {
            NO_DROP_SENTINEL
        } else {
            next_drop_sample_after(&mut rng, 0, AUX_SAMPLE_RATE)
        };

        let initial_params = SineParams {
            amplitude: cli.amplitude,
            frequency: cli.frequency,
            noise: cli.noise,
        };
        let initial_status = 0;
        let initial_enable = 1;

        // RPC table. Like tl-chibi's tl_rpc_finalize(), the introspection
        // methods come first, and the table order determines both the ids
        // reported by rpc.list/rpc.listinfo and the rpc.hash CRC.
        let rpcs = vec![
            RpcSpec::std("rpc.name", Access::RW),
            RpcSpec::std("rpc.id", Access::RW),
            RpcSpec::std("rpc.info", Access::RW),
            RpcSpec::std("rpc.list", Access::RW),
            RpcSpec::std("rpc.listinfo", Access::RW),
            RpcSpec::prop("rpc.hash", Value::Uint(4), Access::READ),
            RpcSpec::prop("dev.name", Value::String, Access::READ),
            RpcSpec::prop("dev.desc", Value::String, Access::READ),
            RpcSpec::action("dev.stop"),
            RpcSpec::std("dev.firmware.upload", Access::WRITE),
            RpcSpec::action("dev.firmware.upgrade"),
            RpcSpec::std("dev.metadata", Access::RW),
            RpcSpec::prop("test.amplitude", Value::Float(8), Access::RW),
            RpcSpec::prop("test.frequency", Value::Float(8), Access::RW),
            RpcSpec::prop("test.noise", Value::Float(8), Access::RW),
            RpcSpec::prop("test.status", Value::Uint(1), Access::RW),
            RpcSpec::prop("test.enable", Value::Uint(1), Access::RW)
                .with_extra_meta(RpcMetaFlags::BOOL.bits()),
            RpcSpec::action("test.go"),
            RpcSpec::std("test.capture", Access::READ)
                .with_extra_meta((RpcMetaFlags::READABLE | RpcMetaFlags::CAPTURE).bits()),
        ];
        let rpc_hash = table::hash(&rpcs);

        Ok(Self {
            socket,
            client: None,
            initial_params,
            params: initial_params,
            initial_status,
            status: initial_status,
            initial_enable,
            enable: initial_enable,
            desc: DEVICE_DESC.to_string(),
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
            no_drop,
            aux_samples_generated: 0,
            aux_sample_number: 0,
            aux_segment_id: 0,
            aux_segment_start_time: start_time,
            aux_pending_segment_update: false,
            next_aux_drop_sample,
            last_heartbeat: Instant::now(),
            next_log_message_at: Instant::now() + next_log_delay(&mut rng),
            next_log_level: 0,
            capture: CaptureBuffer::new(),
            rng,
            rpcs,
            rpc_hash,
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
        if self.no_drop {
            terminal_println!("  random sample drops disabled (--no-drop)");
        } else {
            terminal_println!(
                "  randomly dropping one sample from each sample clock about once per minute"
            );
        }
        terminal_println!(
            "  capture buffer: test.capture(-1) trigger, test.capture(-2) status, \
             test.capture(-3) metadata, {}-{} f32 samples, ~{:.1}s delay",
            CAPTURE_SAMPLE_COUNT_MIN,
            CAPTURE_SAMPLE_COUNT_MAX,
            CAPTURE_TRIGGER_DELAY.as_secs_f64()
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
                    match packet::Packet::from_slice_prefix(&buf[..size]) {
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
        self.next_log_message_at = Instant::now() + self.next_log_delay();
        self.next_log_level = 0;
        self.capture.clear();
    }

    fn reboot(&mut self) -> io::Result<()> {
        self.session_id = self.next_session_id();
        self.params = self.initial_params;
        self.status = self.initial_status;
        self.enable = self.initial_enable;
        self.reset_run();
        terminal_println!("rebooted test device; new session id {}", self.session_id);

        if let Some(client) = self.client {
            self.send_initial_packets(client.addr)?;
        }
        Ok(())
    }

    fn handle_packet(&mut self, packet: packet::Packet, addr: SocketAddr) -> io::Result<()> {
        if let packet::Payload::RpcRequest(req) = packet.payload() {
            self.handle_rpc(req, packet.route(), addr)?;
        }
        Ok(())
    }

    fn handle_rpc(
        &mut self,
        request: rpc::Request<'_>,
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.update_capture();

        let id = request.id.value();
        let arg = request.args;
        let rpc::Method::ByName(name) = request.method else {
            return self.send_rpc_error(id, RpcError::NotFound, routing, addr);
        };
        let Ok(method) = std::str::from_utf8(name) else {
            return self.send_rpc_error(id, RpcError::NotFound, routing, addr);
        };

        match method {
            "dev.name" => self.rpc_read_string(id, DEVICE_NAME, arg, routing, addr),
            "dev.desc" => self.rpc_read_string(id, &self.desc.clone(), arg, routing, addr),
            "dev.stop" => self.send_rpc_reply(id, &[], routing, addr),
            // Accept and acknowledge each firmware chunk (contents ignored).
            "dev.firmware.upload" => self.send_rpc_reply(id, &[], routing, addr),
            // Commit: simulate a reboot into a new build by rewriting dev.desc.
            "dev.firmware.upgrade" => {
                self.desc = "Twinleaf tio-test R1 ((null)) [2026-06-08/000002]".to_string();
                self.send_rpc_reply(id, &[], routing, addr)
            }
            "rpc.hash" => self.rpc_read_u32(id, self.rpc_hash, arg, routing, addr),
            "rpc.name" => self.answer(id, routing, addr, |out| table::name(&self.rpcs, arg, out)),
            "rpc.id" => self.answer(id, routing, addr, |out| table::id(&self.rpcs, arg, out)),
            "rpc.info" => self.answer(id, routing, addr, |out| table::info(&self.rpcs, arg, out)),
            "rpc.list" => self.answer(id, routing, addr, |out| {
                table::list(&self.rpcs, arg, false, out)
            }),
            "rpc.listinfo" => self.answer(id, routing, addr, |out| {
                table::list(&self.rpcs, arg, true, out)
            }),
            "dev.metadata" => self.answer(id, routing, addr, |out| metadata::reply(self, arg, out)),
            "test.amplitude" => {
                let next = self.read_or_write_nonnegative_f64(
                    id,
                    arg,
                    self.params.amplitude,
                    routing,
                    addr,
                )?;
                self.params.amplitude = next;
                Ok(())
            }
            "test.frequency" => {
                let next = self.read_or_write_nonnegative_f64(
                    id,
                    arg,
                    self.params.frequency,
                    routing,
                    addr,
                )?;
                self.params.frequency = next;
                Ok(())
            }
            "test.noise" => {
                let next =
                    self.read_or_write_nonnegative_f64(id, arg, self.params.noise, routing, addr)?;
                self.params.noise = next;
                Ok(())
            }
            "test.status" => {
                let next = self.read_or_write_u8(id, arg, self.status, routing, addr)?;
                self.status = next;
                Ok(())
            }
            "test.enable" => {
                let next = self.read_or_write_u8(id, arg, self.enable, routing, addr)?;
                self.enable = next;
                Ok(())
            }
            "test.go" => self.rpc_action(id, arg, routing, addr),
            "test.capture" => self.rpc_capture(id, arg, routing, addr),
            _ => self.send_rpc_error(id, RpcError::NotFound, routing, addr),
        }
    }

    fn rpc_read_string(
        &self,
        id: u16,
        value: &str,
        arg: &[u8],
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if !arg.is_empty() {
            return self.send_rpc_error(id, RpcError::ReadOnly, routing, addr);
        }
        self.send_rpc_reply(id, value.as_bytes(), routing, addr)
    }

    fn rpc_read_u32(
        &self,
        id: u16,
        value: u32,
        arg: &[u8],
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if !arg.is_empty() {
            return self.send_rpc_error(id, RpcError::ReadOnly, routing, addr);
        }
        self.send_rpc_reply(id, &value.to_le_bytes(), routing, addr)
    }

    /// Send the reply or error an RPC answers with.
    fn answer(
        &self,
        id: u16,
        routing: DeviceRoute,
        addr: SocketAddr,
        answer: impl FnOnce(&mut [u8]) -> Result<usize, RpcError>,
    ) -> io::Result<()> {
        let mut out = [0u8; table::REPLY_MAX];
        match answer(&mut out) {
            Ok(len) => self.send_rpc_reply(id, &out[..len], routing, addr),
            Err(error) => self.send_rpc_error(id, error, routing, addr),
        }
    }

    fn read_or_write_nonnegative_f64(
        &self,
        id: u16,
        arg: &[u8],
        current: f64,
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<f64> {
        let value = match arg.len() {
            0 => current,
            8 => f64::from_le_bytes(arg.try_into().unwrap()),
            _ => {
                self.send_rpc_error(id, RpcError::ArgsSize, routing, addr)?;
                return Ok(current);
            }
        };

        if !value.is_finite() || value < 0.0 {
            self.send_rpc_error(id, RpcError::Invalid, routing, addr)?;
            return Ok(current);
        }

        self.send_rpc_reply(id, &value.to_le_bytes(), routing, addr)?;
        Ok(value)
    }

    fn read_or_write_u8(
        &self,
        id: u16,
        arg: &[u8],
        current: u8,
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<u8> {
        let value = match arg.len() {
            0 => current,
            1 => arg[0],
            _ => {
                self.send_rpc_error(id, RpcError::ArgsSize, routing, addr)?;
                return Ok(current);
            }
        };

        self.send_rpc_reply(id, &[value], routing, addr)?;
        Ok(value)
    }

    fn rpc_action(
        &self,
        id: u16,
        arg: &[u8],
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        if !arg.is_empty() {
            return self.send_rpc_error(id, RpcError::ArgsSize, routing, addr);
        }
        terminal_println!("test.go action invoked");
        self.send_rpc_reply(id, &[], routing, addr)
    }

    fn rpc_capture(
        &mut self,
        id: u16,
        arg: &[u8],
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let mut out = [0u8; table::REPLY_MAX];
        let answered = Selector::parse(arg).and_then(|selector| {
            let len = self.capture.view().reply(selector, &mut out)?;
            Ok((selector, len))
        });
        match answered {
            Ok((Selector::Trigger, len)) => {
                self.trigger_capture();
                self.send_rpc_reply(id, &out[..len], routing, addr)
            }
            Ok((_, len)) => self.send_rpc_reply(id, &out[..len], routing, addr),
            Err(error) => self.send_rpc_error(id, error, routing, addr),
        }
    }

    fn trigger_capture(&mut self) {
        let (data, info) = self.generate_capture_data();
        self.capture
            .begin_capture(data, info, Instant::now() + CAPTURE_TRIGGER_DELAY);
        terminal_println!(
            "test.capture triggered ({} samples); data available in ~{:.1}s",
            info.length,
            CAPTURE_TRIGGER_DELAY.as_secs_f64()
        );
    }

    fn update_capture(&mut self) {
        let was_locked = self.capture.locked();
        self.capture.update(Instant::now());
        if was_locked && !self.capture.locked() {
            terminal_println!("test.capture done ({} bytes)", self.capture.export_size());
        }
    }

    fn generate_capture_data(&mut self) -> (Vec<u8>, CaptureInfo) {
        let sample_count = next_capture_sample_count(&mut self.rng);
        let mut data = Vec::with_capacity(sample_count * CAPTURE_SAMPLE_BYTES);

        let noise_sigma = self.params.noise * (f64::from(self.sample_rate) / 2.0).sqrt();
        let start_sample = self.samples_generated;
        for offset in 0..sample_count as u64 {
            let t = (start_sample + offset) as f64 / f64::from(self.sample_rate);
            let phase = std::f64::consts::TAU * self.params.frequency * t;
            let value =
                self.params.amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
            data.extend((value as f32).to_le_bytes());
        }

        let info = CaptureInfo {
            length: sample_count as u32,
            y_calibration: CAPTURE_Y_CALIBRATION,
            x_offset: start_sample as f32 / self.sample_rate as f32,
            x_stride: 1.0 / self.sample_rate as f32,
        };

        (data, info)
    }

    fn send_periodic_packets(&mut self) -> io::Result<()> {
        self.update_capture();
        let Some(client) = self.client else {
            return Ok(());
        };

        if self.last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
            self.send_heartbeat(client.addr)?;
            self.last_heartbeat = Instant::now();
        }

        self.send_log_message_if_due(client.addr)?;
        self.send_due_samples(client.addr)?;
        self.send_due_aux_samples(client.addr)
    }

    fn send_log_message_if_due(&mut self, addr: SocketAddr) -> io::Result<()> {
        if Instant::now() < self.next_log_message_at {
            return Ok(());
        }

        let level = self.next_log_level();
        let lucky_number = (self.rng.next_u64() % 10_000) as u32;
        let message = self.random_log_message(lucky_number);
        let entry = log::LogMessage {
            level,
            data: lucky_number,
            message: message.as_bytes(),
        };
        self.send_written(
            addr,
            DeviceRoute::root(),
            || format!("log message level={level:?} data={lucky_number}"),
            |buf| entry.write(buf),
        )?;
        self.next_log_message_at = Instant::now() + self.next_log_delay();
        Ok(())
    }

    fn send_due_samples(&mut self, addr: SocketAddr) -> io::Result<()> {
        for _ in 0..4 {
            let due = self.due_samples();
            if due == 0 {
                break;
            }

            self.send_sample_segment_updates_if_needed(addr)?;

            let samples_until_drop = self.next_drop_sample.saturating_sub(self.samples_generated);
            if samples_until_drop == 0 {
                self.drop_sample();
                continue;
            }

            let samples_left_in_segment = u64::from(self.segment_samples - self.sample_number);
            let batch_len = due
                .min(self.max_samples_per_packet)
                .min(samples_left_in_segment)
                .min(samples_until_drop);
            self.send_sample_batches(batch_len, addr)?;
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
            &waveform_data,
            addr,
        )?;
        self.send_stream_packet(
            STATUS_STREAM_ID,
            first_sample_n,
            segment_id,
            &status_data,
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

            self.send_aux_segment_update_if_needed(addr)?;

            let samples_until_drop = self
                .next_aux_drop_sample
                .saturating_sub(self.aux_samples_generated);
            if samples_until_drop == 0 {
                self.drop_aux_sample();
                continue;
            }

            let samples_left_in_segment =
                u64::from(self.aux_segment_samples - self.aux_sample_number);
            let batch_len = due
                .min(self.aux_max_samples_per_packet)
                .min(samples_left_in_segment)
                .min(samples_until_drop);
            self.send_aux_sample_batch(batch_len, addr)?;
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

        self.send_stream_packet(AUX_STREAM_ID, first_sample_n, segment_id, &data, addr)?;

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
        samples: &[u8],
        addr: SocketAddr,
    ) -> io::Result<()> {
        let packet = data::Samples {
            stream_id: StreamId::new(stream_id),
            segment_id: SegmentId::new(segment_id),
            first: SampleNumber::new(first_sample_n),
            data: samples,
        };
        self.send_written(
            addr,
            DeviceRoute::root(),
            || {
                format!(
                    "stream data stream_id={stream_id} segment_id={segment_id} \
                     first_sample_n={first_sample_n} data_bytes={} max_data_bytes={}",
                    samples.len(),
                    stream_data_max_data_bytes()
                )
            },
            |buf| packet.write(buf),
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
        if let Some(addr) = addr {
            self.send_sample_segment_updates_if_needed(addr)?;
        }
        self.drop_sample();

        if let Some(addr) = addr {
            self.send_aux_segment_update_if_needed(addr)?;
        }
        self.drop_aux_sample();

        Ok(())
    }

    fn next_drop_sample_after(&mut self, current_sample: u64, sample_rate: u32) -> u64 {
        if self.no_drop {
            return NO_DROP_SENTINEL;
        }
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

    fn next_log_delay(&mut self) -> Duration {
        next_log_delay(&mut self.rng)
    }

    fn next_log_level(&mut self) -> log::LogLevel {
        let levels = [
            log::LogLevel::CRITICAL,
            log::LogLevel::ERROR,
            log::LogLevel::WARNING,
            log::LogLevel::INFO,
            log::LogLevel::DEBUG,
        ];
        let level = levels[self.next_log_level % levels.len()];
        self.next_log_level = self.next_log_level.wrapping_add(1);
        level
    }

    fn random_log_message(&mut self, lucky_number: u32) -> String {
        let templates = [
            "lucky number {lucky} nudged the simulated flux loop",
            "telemetry monitor reported lucky number {lucky}",
            "calibration check landed on lucky number {lucky}",
            "simulated event counter reached lucky number {lucky}",
            "operator marker recorded lucky number {lucky}",
            "background diagnostic index settled at lucky number {lucky}",
        ];
        let template = templates[(self.rng.next_u64() as usize) % templates.len()];
        template.replace("{lucky}", &lucky_number.to_string())
    }

    fn send_sample_segment_updates_if_needed(&mut self, addr: SocketAddr) -> io::Result<()> {
        if self.pending_segment_update && self.sample_number == 0 {
            for stream_id in [SINE_STREAM_ID, STATUS_STREAM_ID] {
                self.send_metadata(
                    data::Metadata::Segment(self.segment_record(stream_id)),
                    addr,
                )?;
            }
            self.pending_segment_update = false;
        }
        Ok(())
    }

    fn send_aux_segment_update_if_needed(&mut self, addr: SocketAddr) -> io::Result<()> {
        if self.aux_pending_segment_update && self.aux_sample_number == 0 {
            self.send_metadata(
                data::Metadata::Segment(self.segment_record(AUX_STREAM_ID)),
                addr,
            )?;
            self.aux_pending_segment_update = false;
        }
        Ok(())
    }

    fn send_initial_packets(&self, addr: SocketAddr) -> io::Result<()> {
        self.send_rpc_hash_setting(addr)?;
        self.send_heartbeat(addr)?;
        self.send_metadata(data::Metadata::Device(self.device()), addr)?;
        for stream_id in STREAM_IDS {
            let stream = self.stream(stream_id).expect("known stream");
            self.send_metadata(data::Metadata::Stream(stream), addr)?;
            self.send_metadata(
                data::Metadata::Segment(self.segment_record(stream_id)),
                addr,
            )?;
            for index in 0..stream.n_columns {
                let column = self.column(stream_id, index).expect("known column");
                self.send_metadata(data::Metadata::Column(column), addr)?;
            }
        }
        Ok(())
    }

    fn send_heartbeat(&self, addr: SocketAddr) -> io::Result<()> {
        let beat = heartbeat::Heartbeat::Session(SessionId::new(self.session_id));
        self.send_written(
            addr,
            DeviceRoute::root(),
            || format!("heartbeat session={}", self.session_id),
            |buf| beat.write(buf),
        )
    }

    /// The `rpc.hash` broadcast a device sends on connect, so a client knows
    /// which RPC table it is talking to before it asks anything.
    fn send_rpc_hash_setting(&self, addr: SocketAddr) -> io::Result<()> {
        let hash = self.rpc_hash.to_le_bytes();
        let setting = settings::Setting {
            name: b"rpc.hash",
            flags: 0,
            reply: &hash,
        };
        self.send_written(
            addr,
            DeviceRoute::root(),
            || "rpc.hash setting".to_string(),
            |buf| setting.write(buf),
        )
    }

    fn send_metadata(&self, record: data::Metadata<'_>, addr: SocketAddr) -> io::Result<()> {
        self.send_written(
            addr,
            DeviceRoute::root(),
            move || format!("metadata {record:?}"),
            move |buf| record.write(data::MetadataFlags::UPDATE, buf),
        )
    }

    fn send_rpc_reply(
        &self,
        id: u16,
        reply: &[u8],
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.send_written(
            addr,
            routing,
            || format!("rpc reply id={id} reply_bytes={}", reply.len()),
            |buf| rpc::write_reply(buf, RpcRequestId::new(id), reply),
        )
    }

    fn send_rpc_error(
        &self,
        id: u16,
        error: RpcError,
        routing: DeviceRoute,
        addr: SocketAddr,
    ) -> io::Result<()> {
        self.send_written(
            addr,
            routing,
            || format!("rpc error id={id} error={error:?}"),
            |buf| rpc::write_error(buf, RpcRequestId::new(id), error),
        )
    }

    /// Send a packet built by one of twinleaf-proto's device-side writers,
    /// addressed to the device it speaks for: `routing` is empty for this
    /// device itself, and one hop per level for a simulated subtree device.
    fn send_written(
        &self,
        addr: SocketAddr,
        routing: DeviceRoute,
        describe: impl Fn() -> String,
        write: impl FnOnce(&mut [u8]) -> Option<usize>,
    ) -> io::Result<()> {
        let mut buf = [0u8; proto::packet::Packet::MAX_SIZE];
        let mut written = write(&mut buf)
            .and_then(|len| proto::packet::Packet::from_slice(&buf[..len]))
            .ok_or_else(|| encode_error(&describe(), "does not fit a packet"))?;
        // Hops travel leaf-first on the wire, the reverse of a route's order.
        for &hop in routing.as_slice().iter().rev() {
            written
                .push_hop(hop)
                .map_err(|_| encode_error(&describe(), "cannot carry its route"))?;
        }
        self.socket.send_to(written.as_slice(), addr)?;
        Ok(())
    }

    fn segment_record(&self, stream_id: u8) -> data::Segment<'static> {
        let (segment_id, start_time, sampling_rate) = match stream_id {
            AUX_STREAM_ID => (
                self.aux_segment_id,
                self.aux_segment_start_time,
                AUX_SAMPLE_RATE,
            ),
            _ => (self.segment_id, self.segment_start_time, self.sample_rate),
        };

        data::Segment {
            stream_id: StreamId::new(stream_id),
            segment_id: SegmentId::new(segment_id),
            flags: data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE,
            epoch: sync::Epoch::UNIX,
            timeref_serial: DEVICE_SERIAL,
            timeref_session: SessionId::new(self.session_id),
            start_time,
            sampling_rate,
            decimation: 1,
            filter_cutoff: sampling_rate as f32 / 2.0,
            filter_type: data::FilterType::NONE,
        }
    }

    /// The segment record for a segment other than the one being acquired: it
    /// starts one segment length later for each step past the current one.
    fn segment_record_at(&self, stream_id: u8, segment_id: u8) -> data::Segment<'static> {
        let mut segment = self.segment_record(stream_id);
        let ahead = u32::from((segment_id + N_SEGMENTS - segment.segment_id.value()) % N_SEGMENTS);
        segment.start_time = segment
            .start_time
            .saturating_add(ahead.saturating_mul(self.segment_seconds));
        segment.segment_id = SegmentId::new(segment_id);
        segment
    }
}

impl Records for TestDevice {
    fn device(&self) -> data::Device<'_> {
        data::Device {
            session: SessionId::new(self.session_id),
            n_streams: STREAM_IDS.len() as u8,
            name: DEVICE_NAME,
            serial: DEVICE_SERIAL,
            firmware: DEVICE_FIRMWARE,
        }
    }

    fn stream_ids(&self) -> impl Iterator<Item = u8> {
        STREAM_IDS.into_iter()
    }

    fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
        let (name, sample_size, buf_samples) = match stream_id {
            SINE_STREAM_ID => ("sine", SINE_SAMPLE_BYTES, self.sample_rate),
            STATUS_STREAM_ID => ("status", STATUS_SAMPLE_BYTES, self.sample_rate),
            AUX_STREAM_ID => ("aux", AUX_SAMPLE_BYTES, AUX_SAMPLE_RATE),
            _ => return None,
        };
        Some(data::Stream {
            stream_id: StreamId::new(stream_id),
            n_columns: 2,
            n_segments: N_SEGMENTS,
            sample_size: u16::try_from(sample_size).ok()?,
            buf_samples: u16::try_from(buf_samples).ok()?,
            name,
        })
    }

    fn segment(&self, stream_id: u8, index: u8) -> Option<data::Segment<'_>> {
        if !STREAM_IDS.contains(&stream_id) {
            return None;
        }
        Some(match index {
            CURRENT_SEGMENT => self.segment_record(stream_id),
            index => self.segment_record_at(stream_id, index),
        })
    }

    fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>> {
        let (data_type, name, units, description) = match (stream_id, index) {
            (SINE_STREAM_ID, 0) => (data::DataType::F64, "sine", "V", "Noisy sine wave"),
            (SINE_STREAM_ID, 1) => (data::DataType::F64, "cosine", "V", "Noisy quadrature wave"),
            (STATUS_STREAM_ID, 0) => (
                data::DataType::U8,
                "status",
                "",
                "Mirrors the test.status RPC",
            ),
            (STATUS_STREAM_ID, 1) => (
                data::DataType::U8,
                "signal_level",
                "",
                "Fixed simulated signal level",
            ),
            (AUX_STREAM_ID, 0) => (data::DataType::F64, "triangle", "arb", "Triangle wave"),
            (AUX_STREAM_ID, 1) => (data::DataType::F64, "sawtooth", "arb", "Sawtooth wave"),
            _ => return None,
        };
        Some(data::Column {
            stream_id: StreamId::new(stream_id),
            index: ColumnId::new(index),
            data_type,
            name,
            units,
            description,
        })
    }
}

fn encode_error(what: &str, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("could not encode {what}: it {why}"),
    )
}

fn stream_data_max_data_bytes() -> usize {
    proto::packet::Packet::MAX_PAYLOAD.saturating_sub(data::SAMPLE_HEADER_SIZE)
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

fn next_log_delay(rng: &mut GaussianRng) -> Duration {
    let jitter = LOG_MESSAGE_JITTER.mul_f64(rng.next_unit());
    LOG_MESSAGE_MIN_INTERVAL + jitter
}

fn next_capture_sample_count(rng: &mut GaussianRng) -> usize {
    let span = CAPTURE_SAMPLE_COUNT_MAX - CAPTURE_SAMPLE_COUNT_MIN + 1;
    CAPTURE_SAMPLE_COUNT_MIN + (rng.next_u64() as usize % span)
}

fn unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
}

fn unix_time_secs(now: Duration) -> u32 {
    u32::try_from(now.as_secs()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn capture_buffer_exports_indexed_blocks_after_delay() {
        let now = Instant::now();
        let mut capture = CaptureBuffer::new();
        capture.block_size = 4;
        capture.begin_capture(
            (0u8..10).collect(),
            CaptureInfo {
                length: 10,
                ..CaptureInfo::default()
            },
            now + Duration::from_millis(500),
        );

        assert!(capture.locked());
        assert_eq!(capture.status(), capture::Status::Capturing);

        capture.update(now + Duration::from_millis(499));
        assert!(capture.locked());

        capture.update(now + Duration::from_millis(500));
        assert!(!capture.locked());
        assert_eq!(capture.status(), capture::Status::Done);
        assert_eq!(capture.export_size(), 10);
        assert_eq!(capture.info().length, 10);
        assert_eq!(capture.view().block(2), Some(&[8, 9][..]));
    }

    #[test]
    fn default_capture_block_size_fits_rpc_replies() {
        assert!(usize::from(CAPTURE_DEFAULT_BLOCK_SIZE) <= table::REPLY_MAX);
    }

    #[test]
    fn metadata_bootstrap_replies_in_sweep_order_within_capacity() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0"]);
        let device = TestDevice::new(cli).unwrap();

        let mut out = [0u8; table::REPLY_MAX];
        let len = metadata::reply(&device, &[], &mut out).unwrap();
        let kinds: Vec<_> = data::MetadataReply::parse(&out[..len])
            .unwrap()
            .map(|(kind, _)| kind)
            .collect();
        use data::MetadataType;
        let mut expected = vec![MetadataType::Device];
        for _ in STREAM_IDS {
            expected.extend([
                MetadataType::Stream,
                MetadataType::Segment,
                MetadataType::Column,
                MetadataType::Column,
            ]);
        }
        assert_eq!(kinds, expected);
    }

    #[test]
    fn metadata_selectors_resolve_the_current_segment_sentinel() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0"]);
        let device = TestDevice::new(cli).unwrap();

        let segment = device.segment(SINE_STREAM_ID, CURRENT_SEGMENT).unwrap();
        assert_eq!(segment.segment_id.value(), device.segment_id);
        assert!(device.stream(99).is_none());
        assert!(device.segment(99, CURRENT_SEGMENT).is_none());
    }

    #[test]
    fn capture_data_uses_current_sine_parameters() {
        let cli = SimulateCli::parse_from([
            "tio-simulate",
            "--port",
            "0",
            "--samplerate",
            "4",
            "--frequency",
            "1",
            "--amplitude",
            "2",
            "--noise",
            "0",
        ]);
        let mut device = TestDevice::new(cli).unwrap();

        let (data, info) = device.generate_capture_data();

        assert!(
            (CAPTURE_SAMPLE_COUNT_MIN as u32..=CAPTURE_SAMPLE_COUNT_MAX as u32)
                .contains(&info.length)
        );
        assert_eq!(data.len(), info.length as usize * CAPTURE_SAMPLE_BYTES);
        assert_eq!(info.y_calibration, 1.0);
        assert_eq!(info.x_offset, 0.0);
        assert_eq!(info.x_stride, 0.25);

        let first = f32::from_le_bytes(data[0..4].try_into().unwrap());
        let second = f32::from_le_bytes(data[4..8].try_into().unwrap());
        assert_eq!(first, 0.0);
        assert!((second - 2.0).abs() < f32::EPSILON);
    }

    #[test]
    fn capture_metadata_uses_tl_chibi_type_and_y_calibration() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0"]);
        let mut device = TestDevice::new(cli).unwrap();
        let (data, info) = device.generate_capture_data();
        let data_len = data.len();
        device
            .capture
            .begin_capture(data, info, Instant::now() + Duration::from_millis(1));
        device
            .capture
            .update(Instant::now() + Duration::from_millis(1));

        let mut out = [0u8; table::REPLY_MAX];
        let len = device
            .capture
            .view()
            .reply(Selector::Metadata, &mut out)
            .unwrap();
        let metadata = CaptureMetadata::parse(&out[..len]).unwrap();

        assert_eq!(metadata.version, METADATA_VERSION);
        assert_eq!(metadata.data_type, data::DataType::F32);
        assert_eq!(metadata.data_size, u32::try_from(data_len).unwrap());
        assert_eq!(metadata.length, info.length);
        assert_eq!(metadata.y_calibration, CAPTURE_Y_CALIBRATION);
    }

    #[test]
    fn capture_sample_count_varies_within_range() {
        let mut rng = GaussianRng::new(1);
        let mut counts = Vec::new();
        for _ in 0..8 {
            counts.push(next_capture_sample_count(&mut rng));
        }

        assert!(counts
            .iter()
            .all(|count| (CAPTURE_SAMPLE_COUNT_MIN..=CAPTURE_SAMPLE_COUNT_MAX).contains(count)));
        assert!(counts.windows(2).any(|pair| pair[0] != pair[1]));
    }
}

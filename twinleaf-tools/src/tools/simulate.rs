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
use std::num::NonZeroU32;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::proto;
use twinleaf::proto::capture::{CaptureMetadata, METADATA_VERSION};
use twinleaf::proto::packet::PacketView;
use twinleaf::proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf::proto::{data, log, sync};
use twinleaf::proto::{SessionId, StreamId};
use twinleaf_device::capture::{self, Capture, Selector};
use twinleaf_device::device::{Call, Device, Handled, Identity};
use twinleaf_device::rpc::{put, Access, Reply, RpcSpec, Value};
use twinleaf_device::segments::{Params, Timeref};
use twinleaf_device::stream::{ColumnDef, Stream, StreamDef};
use twinleaf_device::Sink;

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

/// The streams this device publishes, in the order it describes them.
#[derive(Clone, Copy)]
enum Signal {
    Sine,
    Status,
    Aux,
}

/// The two sample clocks: one the sine and status streams share, one for aux.
const WAVE_CLOCK: usize = 0;
const AUX_CLOCK: usize = 1;
const SEGMENTS: usize = 4;
const DEVICE_NAME: &str = "tio-test";
// The simulator is a development/test device, so its build version is marked
// `DEV`. Format: `{vendor} {name} {revision} ({serial}) [{date}/{build}]`.
const DEVICE_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000001-DEV]";
const DEVICE_SERIAL: &str = "SIM0001";
const DEVICE_FIRMWARE: &str = "twinleaf-rust-test";
const SIGNAL_LEVEL: u8 = 234;
const AUX_SAMPLE_RATE: NonZeroU32 = NonZeroU32::new(25).expect("a positive rate");
const AUX_WAVE_FREQUENCY: f64 = 0.25;
const SAMPLE_DROP_INTERVAL_SECONDS: f64 = 60.0;
const SAMPLE_DROP_JITTER_SECONDS: f64 = 30.0;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
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

const UPGRADED_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000002]";

const SINE_DEF: StreamDef = StreamDef {
    name: "sine",
    columns: &[
        ColumnDef {
            name: "sine",
            units: "V",
            data_type: data::DataType::F64,
            description: "Noisy sine wave",
        },
        ColumnDef {
            name: "cosine",
            units: "V",
            data_type: data::DataType::F64,
            description: "Noisy quadrature wave",
        },
    ],
};

const STATUS_DEF: StreamDef = StreamDef {
    name: "status",
    columns: &[
        ColumnDef {
            name: "status",
            units: "",
            data_type: data::DataType::U8,
            description: "Mirrors the test.status RPC",
        },
        ColumnDef {
            name: "signal_level",
            units: "",
            data_type: data::DataType::U8,
            description: "Fixed simulated signal level",
        },
    ],
};

const AUX_DEF: StreamDef = StreamDef {
    name: "aux",
    columns: &[
        ColumnDef {
            name: "triangle",
            units: "arb",
            data_type: data::DataType::F64,
            description: "Triangle wave",
        },
        ColumnDef {
            name: "sawtooth",
            units: "arb",
            data_type: data::DataType::F64,
            description: "Sawtooth wave",
        },
    ],
};

/// The RPC table. Introspection first, like tl-chibi, and the order fixes
/// the ids `rpc.list` reports and the `rpc.hash`.
static RPCS: [RpcSpec; 20] = [
    RpcSpec::std("rpc.name", Access::RW),
    RpcSpec::std("rpc.id", Access::RW),
    RpcSpec::std("rpc.info", Access::RW),
    RpcSpec::std("rpc.list", Access::RW),
    RpcSpec::std("rpc.listinfo", Access::RW),
    RpcSpec::prop("rpc.hash", Value::Uint(4), Access::READ),
    RpcSpec::prop("dev.name", Value::String, Access::READ),
    RpcSpec::prop("dev.desc", Value::String, Access::READ),
    RpcSpec::prop("dev.session", Value::Uint(4), Access::READ),
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
        .with_extra_meta(RpcMetaFlags::READABLE.union(RpcMetaFlags::CAPTURE).bits()),
];

#[derive(Clone, Copy)]
struct SineParams {
    amplitude: f64,
    frequency: f64,
    noise: f64,
}

/// A sample clock: the streams it drives, how far the run has got, and when
/// it next drops a sample.
struct Clock {
    rate: NonZeroU32,
    segment_samples: u64,
    streams: &'static [Signal],
    generated: u64,
    next_drop: Option<u64>,
}

impl Clock {
    fn new(rate: NonZeroU32, seconds: u32, streams: &'static [Signal]) -> io::Result<Self> {
        let samples = rate
            .get()
            .checked_mul(seconds)
            .filter(|samples| *samples <= data::MAX_SAMPLE_NUMBER)
            .ok_or_else(|| {
                invalid_input("segment contains too many samples for TIO sample numbering")
            })?;
        Ok(Self {
            rate,
            segment_samples: u64::from(samples),
            streams,
            generated: 0,
            next_drop: None,
        })
    }

    /// How many samples it owes the run started at `started_at`.
    fn due(&self, started_at: Instant) -> u64 {
        let elapsed = started_at.elapsed().as_secs_f64();
        ((elapsed * f64::from(self.rate.get())).floor() as u64).saturating_sub(self.generated)
    }

    /// Whether the sample it is about to take starts a new segment.
    fn rolls_over(&self) -> bool {
        self.generated > 0 && self.generated.is_multiple_of(self.segment_samples)
    }
}

#[derive(Clone, Copy)]
struct Client {
    addr: SocketAddr,
    last_rx: Instant,
}

/// The device's packets go to the connected client, and the first send
/// failure is kept for the caller.
struct UdpSink<'a> {
    socket: &'a UdpSocket,
    addr: SocketAddr,
    failed: Option<io::Error>,
}

impl<'a> UdpSink<'a> {
    fn new(socket: &'a UdpSocket, addr: SocketAddr) -> Self {
        Self {
            socket,
            addr,
            failed: None,
        }
    }

    fn finish(self) -> io::Result<()> {
        self.failed.map_or(Ok(()), Err)
    }
}

impl Sink for UdpSink<'_> {
    fn send(&mut self, packet: &[u8]) {
        if self.failed.is_none() {
            self.failed = self.socket.send_to(packet, self.addr).err();
        }
    }
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
    segment_seconds: u32,
    streams: [Stream<SEGMENTS>; 3],
    clocks: [Clock; 2],
    device: Device<'static>,
    epoch: Instant,
    started_at: Instant,
    no_drop: bool,
    next_log_message_at: Instant,
    next_log_level: usize,
    capture: CaptureBuffer,
    rng: GaussianRng,
}

impl TestDevice {
    fn new(cli: SimulateCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;

        let now = unix_duration();
        let seed = now.as_nanos() as u64 ^ u64::from(cli.port).rotate_left(32);
        let session_id = (seed as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        let sample_rate = NonZeroU32::new(cli.samplerate)
            .ok_or_else(|| invalid_input("sample rate must be at least one hertz"))?;
        let clocks = [
            Clock::new(
                sample_rate,
                cli.segment_seconds,
                &[Signal::Sine, Signal::Status],
            )?,
            Clock::new(AUX_SAMPLE_RATE, cli.segment_seconds, &[Signal::Aux])?,
        ];

        let mut rng = GaussianRng::new(seed | 1);
        let initial_params = SineParams {
            amplitude: cli.amplitude,
            frequency: cli.frequency,
            noise: cli.noise,
        };
        let initial_status = 0;
        let initial_enable = 1;

        let identity = Identity::new(DEVICE_NAME, DEVICE_DESC, DEVICE_SERIAL, DEVICE_FIRMWARE)
            .ok_or_else(|| invalid_input("identity too long"))?;

        Ok(Self {
            socket,
            client: None,
            initial_params,
            params: initial_params,
            initial_status,
            status: initial_status,
            initial_enable,
            enable: initial_enable,
            segment_seconds: cli.segment_seconds,
            streams: boot_streams(sample_rate)?,
            clocks,
            device: Device::new(identity, SessionId::new(session_id), &RPCS),
            epoch: Instant::now(),
            started_at: Instant::now(),
            no_drop: cli.no_drop,
            next_log_message_at: Instant::now() + next_log_delay(&mut rng),
            next_log_level: 0,
            capture: CaptureBuffer::new(),
            rng,
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
            self.sample_rate(),
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
                    KeyCode::Char('d') => self.drop_samples_now(),
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
                    match PacketView::parse_prefix(&buf[..size]) {
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
                self.connected(addr)?;
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

    /// Restart acquisition: every stream stops and starts again at a fresh
    /// time reference, so its samples begin at zero in a new segment.
    fn reset_run(&mut self) {
        self.started_at = Instant::now();
        let timeref = Timeref::new(
            sync::Epoch::UNIX,
            u32::try_from(unix_duration().as_secs()).unwrap_or(u32::MAX),
            self.device.session,
            DEVICE_SERIAL,
        )
        .expect("the device serial fits a time reference");
        for stream in &mut self.streams {
            stream.stop();
            stream
                .start(timeref.clone())
                .expect("a stopped stream starts");
        }
        for clock in 0..self.clocks.len() {
            self.clocks[clock].generated = 0;
            self.clocks[clock].next_drop = self.next_drop(clock);
        }
        self.next_log_message_at = Instant::now() + next_log_delay(&mut self.rng);
        self.next_log_level = 0;
        self.capture.clear();
    }

    /// Power-cycle: a new session, the settings a device boots with, and a
    /// fresh segment ring on every stream.
    fn reboot(&mut self) -> io::Result<()> {
        self.device.session = SessionId::new(self.next_session_id());
        self.params = self.initial_params;
        self.status = self.initial_status;
        self.enable = self.initial_enable;
        self.streams = boot_streams(self.sample_rate())?;
        self.reset_run();
        terminal_println!(
            "rebooted test device; new session id {}",
            self.device.session.value()
        );

        if let Some(client) = self.client {
            self.connected(client.addr)?;
        }
        Ok(())
    }

    fn connected(&mut self, addr: SocketAddr) -> io::Result<()> {
        let now = self.now_ms();
        let mut sink = UdpSink::new(&self.socket, addr);
        self.device.connected(&self.streams[..], now, &mut sink);
        sink.finish()
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn handle_packet(&mut self, packet: PacketView<'_>, addr: SocketAddr) -> io::Result<()> {
        self.update_capture();
        let mut sink = UdpSink::new(&self.socket, addr);
        let call = match self.device.handle(&self.streams[..], packet, &mut sink) {
            Handled::Done => return sink.finish(),
            Handled::Rpc(call) => call,
        };
        sink.finish()?;

        let result = self.app_rpc(&call);
        let mut sink = UdpSink::new(&self.socket, addr);
        call.reply(result.as_deref().map_err(|error| *error), &mut sink);
        sink.finish()
    }

    /// The RPCs this device adds to the standard ones.
    fn app_rpc(&mut self, call: &Call<'_, '_>) -> Result<Reply, RpcError> {
        let args = call.args;
        let mut reply = Reply::new();
        match call.name {
            "dev.stop" | "dev.firmware.upload" => {}
            "dev.firmware.upgrade" => {
                self.device.identity.desc =
                    UPGRADED_DESC.try_into().map_err(|_| RpcError::Internal)?;
            }
            "test.amplitude" => {
                self.params.amplitude = nonnegative_f64(args, self.params.amplitude)?;
                put(&mut reply, &self.params.amplitude.to_le_bytes())?;
            }
            "test.frequency" => {
                self.params.frequency = nonnegative_f64(args, self.params.frequency)?;
                put(&mut reply, &self.params.frequency.to_le_bytes())?;
            }
            "test.noise" => {
                self.params.noise = nonnegative_f64(args, self.params.noise)?;
                put(&mut reply, &self.params.noise.to_le_bytes())?;
            }
            "test.status" => {
                self.status = u8_property(args, self.status)?;
                put(&mut reply, &[self.status])?;
            }
            "test.enable" => {
                self.enable = u8_property(args, self.enable)?;
                put(&mut reply, &[self.enable])?;
            }
            "test.go" => {
                if !args.is_empty() {
                    return Err(RpcError::ArgsSize);
                }
                terminal_println!("test.go action invoked");
            }
            "test.capture" => {
                let selector = Selector::parse(args)?;
                self.capture.view().reply(selector, &mut reply)?;
                if selector == Selector::Trigger {
                    self.trigger_capture();
                }
            }
            _ => return Err(RpcError::NotFound),
        }
        Ok(reply)
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

        let rate = self.sample_rate().get();
        let noise_sigma = self.params.noise * (f64::from(rate) / 2.0).sqrt();
        let start_sample = self.clocks[WAVE_CLOCK].generated;
        for offset in 0..sample_count as u64 {
            let t = (start_sample + offset) as f64 / f64::from(rate);
            let phase = std::f64::consts::TAU * self.params.frequency * t;
            let value =
                self.params.amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
            data.extend((value as f32).to_le_bytes());
        }

        let info = CaptureInfo {
            length: sample_count as u32,
            y_calibration: CAPTURE_Y_CALIBRATION,
            x_offset: start_sample as f32 / rate as f32,
            x_stride: 1.0 / rate as f32,
        };

        (data, info)
    }

    fn send_periodic_packets(&mut self) -> io::Result<()> {
        self.update_capture();
        let Some(client) = self.client else {
            return Ok(());
        };

        let now = self.now_ms();
        let mut sink = UdpSink::new(&self.socket, client.addr);
        self.device.tick(now, &mut sink);
        sink.finish()?;

        self.send_log_message_if_due(client.addr)?;
        self.send_due_samples(client.addr)
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
        let mut buf = [0u8; proto::packet::Packet::MAX_SIZE];
        let len = entry
            .write(&mut buf)
            .ok_or_else(|| invalid_input("log message does not fit a packet"))?;
        let mut sink = UdpSink::new(&self.socket, addr);
        sink.send(&buf[..len]);
        sink.finish()?;
        self.next_log_message_at = Instant::now() + next_log_delay(&mut self.rng);
        Ok(())
    }

    fn send_due_samples(&mut self, addr: SocketAddr) -> io::Result<()> {
        (0..self.clocks.len()).try_for_each(|clock| self.send_clock_samples(clock, addr))
    }

    /// The samples one clock owes: its streams roll over at a segment
    /// boundary, skip the sample it owes a gap, and publish the rest.
    fn send_clock_samples(&mut self, clock: usize, addr: SocketAddr) -> io::Result<()> {
        for _ in 0..self.clocks[clock].due(self.started_at) {
            if self.clocks[clock].rolls_over() {
                self.rollover(clock);
            }
            if self.clocks[clock].next_drop == Some(self.clocks[clock].generated) {
                self.drop_sample(clock);
                continue;
            }
            self.push_samples(clock, addr)?;
            self.clocks[clock].generated += 1;
        }
        self.flush_streams(clock, addr)
    }

    /// One sample into each stream the clock drives.
    fn push_samples(&mut self, clock: usize, addr: SocketAddr) -> io::Result<()> {
        for &signal in self.clocks[clock].streams {
            let sample = self.sample(signal);
            let size = self.streams[signal as usize].def().sample_size();
            let mut sink = UdpSink::new(&self.socket, addr);
            self.streams[signal as usize].push(&sample[..size], &mut sink);
            sink.finish()?;
        }
        Ok(())
    }

    fn flush_streams(&mut self, clock: usize, addr: SocketAddr) -> io::Result<()> {
        let mut sink = UdpSink::new(&self.socket, addr);
        for &signal in self.clocks[clock].streams {
            self.streams[signal as usize].flush(&mut sink);
        }
        sink.finish()
    }

    fn rollover(&mut self, clock: usize) {
        for &signal in self.clocks[clock].streams {
            self.streams[signal as usize].rollover();
        }
    }

    /// One sample of `signal`, packed in column order at the front of the
    /// widest sample the device takes.
    fn sample(&mut self, signal: Signal) -> [u8; 16] {
        match signal {
            Signal::Sine => self.sine_sample(),
            Signal::Status => {
                let mut sample = [0u8; 16];
                sample[..2].copy_from_slice(&[self.status, SIGNAL_LEVEL]);
                sample
            }
            Signal::Aux => self.aux_sample(),
        }
    }

    /// The rate of the streams the wave clock drives.
    fn sample_rate(&self) -> NonZeroU32 {
        self.clocks[WAVE_CLOCK].rate
    }

    fn sine_sample(&mut self) -> [u8; 16] {
        let rate = f64::from(self.sample_rate().get());
        let t = self.clocks[WAVE_CLOCK].generated as f64 / rate;
        let phase = std::f64::consts::TAU * self.params.frequency * t;
        let noise_sigma = self.params.noise * (rate / 2.0).sqrt();
        let sine = self.params.amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
        let cosine = self.params.amplitude * phase.cos() + noise_sigma * self.rng.next_gaussian();
        let mut sample = [0u8; 16];
        sample[..8].copy_from_slice(&sine.to_le_bytes());
        sample[8..].copy_from_slice(&cosine.to_le_bytes());
        sample
    }

    fn aux_sample(&mut self) -> [u8; 16] {
        let t = self.clocks[AUX_CLOCK].generated as f64 / f64::from(AUX_SAMPLE_RATE.get());
        let phase = (AUX_WAVE_FREQUENCY * t).fract();
        let triangle = 1.0 - 4.0 * (phase - 0.5).abs();
        let sawtooth = 2.0 * phase - 1.0;
        let mut sample = [0u8; 16];
        sample[..8].copy_from_slice(&triangle.to_le_bytes());
        sample[8..].copy_from_slice(&sawtooth.to_le_bytes());
        sample
    }

    /// A dropped sample is the one gap a segment's sample numbers may have.
    fn drop_sample(&mut self, clock: usize) {
        let ids = self.clocks[clock]
            .streams
            .iter()
            .map(|&signal| self.streams[signal as usize].id().to_string())
            .collect::<Vec<String>>()
            .join("/");
        terminal_println!(
            "dropped sample {} from stream {ids}",
            self.clocks[clock].generated
        );
        for &signal in self.clocks[clock].streams {
            self.streams[signal as usize].skip(1);
        }
        self.clocks[clock].generated += 1;
        self.clocks[clock].next_drop = self.next_drop(clock);
    }

    fn drop_samples_now(&mut self) {
        for clock in 0..self.clocks.len() {
            if self.clocks[clock].rolls_over() {
                self.rollover(clock);
            }
            self.drop_sample(clock);
        }
    }

    /// The sample a clock drops next, `None` while `--no-drop` holds.
    fn next_drop(&mut self, clock: usize) -> Option<u64> {
        if self.no_drop {
            return None;
        }
        let (generated, rate) = (self.clocks[clock].generated, self.clocks[clock].rate.get());
        let seconds = SAMPLE_DROP_INTERVAL_SECONDS - SAMPLE_DROP_JITTER_SECONDS
            + self.rng.next_unit() * SAMPLE_DROP_JITTER_SECONDS * 2.0;
        let interval = (seconds * f64::from(rate)).round().max(1.0) as u64;
        Some(generated.saturating_add(interval))
    }

    fn next_session_id(&mut self) -> u32 {
        let mut session_id = (self.rng.next_u64() as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        if session_id == self.device.session.value() {
            session_id = session_id.wrapping_add(1);
        }
        session_id
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
}

/// The streams a boot starts: no decimation, a cutoff at Nyquist, and a
/// fresh segment ring on each.
fn boot_streams(rate: NonZeroU32) -> io::Result<[Stream<SEGMENTS>; 3]> {
    let stream = |id: u8, def: &'static StreamDef, rate: NonZeroU32| {
        let params = Params {
            rate,
            decimation: NonZeroU32::MIN,
            cutoff: rate.get() as f32 / 2.0,
            enabled: true,
        };
        Stream::new(StreamId::new(id), def, params)
            .ok_or_else(|| invalid_input("stream sample is too large for a TIO packet"))
    };
    Ok([
        stream(1, &SINE_DEF, rate)?,
        stream(2, &STATUS_DEF, rate)?,
        stream(3, &AUX_DEF, AUX_SAMPLE_RATE)?,
    ])
}

/// A non-negative f64 property: no argument reads it, eight bytes write it.
fn nonnegative_f64(args: &[u8], current: f64) -> Result<f64, RpcError> {
    let value = match args.len() {
        0 => current,
        8 => f64::from_le_bytes(args.try_into().unwrap()),
        _ => return Err(RpcError::ArgsSize),
    };
    (value.is_finite() && value >= 0.0)
        .then_some(value)
        .ok_or(RpcError::Invalid)
}

/// A u8 property: no argument reads it, one byte writes it.
fn u8_property(args: &[u8], current: u8) -> Result<u8, RpcError> {
    match args {
        [] => Ok(current),
        [value] => Ok(*value),
        _ => Err(RpcError::ArgsSize),
    }
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use twinleaf::proto::data::CURRENT_SEGMENT;
    use twinleaf_device::metadata::{self, Streams};
    use twinleaf_device::rpc::REPLY_MAX;

    /// The segment the sine stream is acquiring.
    fn sine(device: &TestDevice) -> &twinleaf_device::segments::Segment {
        device.streams[Signal::Sine as usize].current()
    }

    /// A device with its streams acquiring, as a connecting client leaves it.
    fn device(args: &[&str]) -> TestDevice {
        let cli = SimulateCli::parse_from([&["tio-simulate", "--port", "0"], args].concat());
        let mut device = TestDevice::new(cli).unwrap();
        device.reset_run();
        device
    }

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
        assert!(usize::from(CAPTURE_DEFAULT_BLOCK_SIZE) <= REPLY_MAX);
    }

    #[test]
    fn every_stream_fits_one_metadata_bootstrap() {
        let device = device(&[]);
        let streams = &device.streams[..];
        let mut out = Reply::new();
        metadata::reply(device.device.record(streams), streams, &[], &mut out).unwrap();
        let kinds: Vec<_> = data::MetadataReply::parse(&out)
            .unwrap()
            .map(|(kind, _)| kind)
            .collect();
        assert_eq!(kinds[0], data::MetadataType::Device);
        assert_eq!(kinds.len(), 1 + device.streams.len() * 4);
    }

    #[test]
    fn metadata_reports_the_ring_and_the_current_segment() {
        let mut device = device(&["--samplerate", "4", "--segment-seconds", "1"]);
        let addr = "127.0.0.1:1".parse().unwrap();
        device.send_due_samples(addr).unwrap();

        let streams = &device.streams[..];
        let record = streams.stream(1).unwrap();
        assert_eq!(record.n_segments, SEGMENTS as u8);
        assert_eq!(record.buf_samples, 0);
        assert_eq!(record.sample_size, 16);
        let segment = streams.segment(1, CURRENT_SEGMENT).unwrap();
        assert_eq!(segment.segment_id.value(), 0);
        assert_eq!(segment.sampling_rate, 4);
        assert_eq!(segment.filter_cutoff, 2.0);
        assert_eq!(streams.segment(1, 0), Some(segment));
        assert_eq!(streams.segment(1, 1), None);
        assert!(streams.stream(99).is_none());
        assert!(streams.segment(99, CURRENT_SEGMENT).is_none());
    }

    #[test]
    fn a_clock_reaching_its_segment_length_rolls_over() {
        let mut device = device(&["--samplerate", "4", "--segment-seconds", "1"]);
        let addr = "127.0.0.1:1".parse().unwrap();
        let start_time = sine(&device).timeref().start_time;
        for _ in 0..=device.clocks[WAVE_CLOCK].segment_samples {
            if device.clocks[WAVE_CLOCK].rolls_over() {
                device.rollover(WAVE_CLOCK);
            }
            device.push_samples(WAVE_CLOCK, addr).unwrap();
            device.clocks[WAVE_CLOCK].generated += 1;
        }

        assert_eq!(sine(&device).id().value(), 1);
        assert_eq!(sine(&device).timeref().start_time, start_time + 1);
    }

    #[test]
    fn a_reboot_starts_a_fresh_ring_where_a_reconnect_takes_the_next_segment() {
        let mut device = device(&["--samplerate", "4", "--segment-seconds", "1"]);
        let addr = "127.0.0.1:1".parse().unwrap();
        device.push_samples(WAVE_CLOCK, addr).unwrap();

        device.reset_run();
        assert_eq!(sine(&device).id().value(), 1);

        let session = device.device.session;
        device.reboot().unwrap();
        assert_ne!(device.device.session, session);
        assert!(device
            .streams
            .iter()
            .all(|stream| stream.current().id().value() == 0));
    }

    #[test]
    fn capture_data_uses_current_sine_parameters() {
        let mut device = device(&[
            "--samplerate",
            "4",
            "--frequency",
            "1",
            "--amplitude",
            "2",
            "--noise",
            "0",
        ]);

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
        let mut device = device(&[]);
        let (data, info) = device.generate_capture_data();
        let data_len = data.len();
        device
            .capture
            .begin_capture(data, info, Instant::now() + Duration::from_millis(1));
        device
            .capture
            .update(Instant::now() + Duration::from_millis(1));

        let mut out = Reply::new();
        device
            .capture
            .view()
            .reply(Selector::Metadata, &mut out)
            .unwrap();
        let metadata = CaptureMetadata::parse(&out).unwrap();

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

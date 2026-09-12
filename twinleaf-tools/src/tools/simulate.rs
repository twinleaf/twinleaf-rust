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
use twinleaf::proto::capture::{CaptureMetadata, METADATA_VERSION};
use twinleaf::proto::packet::{PacketType, PacketView};
use twinleaf::proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf::proto::{data, log, sync};
use twinleaf::proto::{SessionId, StreamId};
use twinleaf_device::capture::{self, Capture, Selector};
use twinleaf_device::device::{Call, Device, Handled, Identity};
use twinleaf_device::rpc::{put, Access, Kind, Reply, RpcSpec};
use twinleaf_device::segments::{Params, Timeref};
use twinleaf_device::settings::Setting;
use twinleaf_device::stream::{ColumnDef, Stream, StreamDef};
use twinleaf_device::sync::{
    ticks, AcquisitionAction, AcquisitionError, Actions, CounterDomain, PulseConfig, Reference,
    ReferenceIdentity, ScheduledEdge, Synchronizer,
};
use twinleaf_device::Sink;

pub fn run_simulate(cli: SimulateCli) -> eyre::Result<()> {
    let mut runtime = Runtime::new(cli)?;
    runtime.run()?;
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
const LOG_MESSAGE_MIN_INTERVAL_NS: u64 = 1_500_000_000;
const LOG_MESSAGE_JITTER_NS: u64 = 4_000_000_000;
const CAPTURE_TRIGGER_DELAY_NS: u64 = 500_000_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// The simulated capture counter: one megahertz, with the fake PPS always on
/// the same tick of it.
const PPS_PERIOD: u32 = 1_000_000;
const PPS_EDGE: u32 = 0;
/// A millisecond, which is as close as a software pulse lands.
const PPS_TOLERANCE_NS: u32 = 1_000_000;
/// Seconds from boot to an automatic start, as `dev.autostart` has it.
const AUTOSTART_SECONDS: u8 = 2;
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
static RPCS: [RpcSpec; 26] = [
    RpcSpec::std("rpc.name", Access::RW),
    RpcSpec::std("rpc.id", Access::RW),
    RpcSpec::std("rpc.info", Access::RW),
    RpcSpec::std("rpc.list", Access::RW),
    RpcSpec::std("rpc.listinfo", Access::RW),
    RpcSpec::prop("rpc.hash", Kind::Uint(4), Access::READ),
    RpcSpec::prop("dev.name", Kind::String, Access::READ),
    RpcSpec::prop("dev.desc", Kind::String, Access::READ),
    RpcSpec::prop("dev.session", Kind::Uint(4), Access::READ),
    RpcSpec::prop("dev.loglevel", Kind::Uint(1), Access::RW),
    RpcSpec::action("dev.start"),
    RpcSpec::action("dev.stop"),
    RpcSpec::action("dev.restart"),
    RpcSpec::prop("dev.autostart", Kind::Uint(1), Access::RW),
    RpcSpec::std("dev.firmware.upload", Access::WRITE),
    RpcSpec::action("dev.firmware.upgrade"),
    RpcSpec::std("dev.metadata", Access::RW),
    RpcSpec::prop("settings.version", Kind::Uint(4), Access::READ),
    RpcSpec::prop("sync.status", Kind::Uint(1), Access::READ),
    RpcSpec::prop("test.amplitude", Kind::Float(8), Access::RW),
    RpcSpec::prop("test.frequency", Kind::Float(8), Access::RW),
    RpcSpec::prop("test.noise", Kind::Float(8), Access::RW),
    RpcSpec::prop("test.status", Kind::Uint(1), Access::RW),
    RpcSpec::prop("test.enable", Kind::Bool, Access::RW),
    RpcSpec::action("test.go"),
    RpcSpec::std("test.capture", Access::READ)
        .with_extra_meta(RpcMetaFlags::READABLE.union(RpcMetaFlags::CAPTURE).bits()),
];

/// The values the `test.*` RPCs read and write.
struct Settings {
    amplitude: Setting<f64>,
    frequency: Setting<f64>,
    noise: Setting<f64>,
    status: Setting<u8>,
    enable: Setting<bool>,
    autostart: Setting<u8>,
}

impl Settings {
    fn new(cli: &SimulateCli) -> Self {
        Self {
            amplitude: Setting::new("test.amplitude", cli.amplitude).checked(nonnegative),
            frequency: Setting::new("test.frequency", cli.frequency).checked(nonnegative),
            noise: Setting::new("test.noise", cli.noise).checked(nonnegative),
            status: Setting::new("test.status", 0),
            enable: Setting::new("test.enable", true),
            autostart: Setting::new("dev.autostart", AUTOSTART_SECONDS),
        }
    }

    /// Go back to the values a boot starts from.
    fn reset(&mut self) {
        self.amplitude.reset();
        self.frequency.reset();
        self.noise.reset();
        self.status.reset();
        self.enable.reset();
        self.autostart.reset();
    }
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

    /// How many samples it owes after `elapsed` nanoseconds of the run.
    fn due(&self, elapsed: u64) -> u64 {
        let seconds = elapsed as f64 / 1_000_000_000.0;
        ((seconds * f64::from(self.rate.get())).floor() as u64).saturating_sub(self.generated)
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

/// Where the device's packets go: the connected client, or nowhere while
/// there is none. The first send failure is kept for the caller.
struct UdpSink<'a> {
    socket: &'a UdpSocket,
    client: Option<SocketAddr>,
    failed: Option<io::Error>,
}

impl<'a> UdpSink<'a> {
    fn new(socket: &'a UdpSocket, client: Option<SocketAddr>) -> Self {
        Self {
            socket,
            client,
            failed: None,
        }
    }

    fn finish(self) -> io::Result<()> {
        self.failed.map_or(Ok(()), Err)
    }
}

impl Sink for UdpSink<'_> {
    fn send(&mut self, packet: &[u8]) {
        if let (Some(addr), None) = (self.client, &self.failed) {
            self.failed = self.socket.send_to(packet, addr).err();
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
    ready_at: u64,
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

    fn begin_capture(&mut self, data: Vec<u8>, info: CaptureInfo, ready_at: u64) {
        self.capturing = Some(CapturingCapture {
            ready_at,
            data,
            info,
        });
    }

    fn update(&mut self, now: u64) {
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

/// One acquisition RPC's effect on the synchronizer.
type Acquisition = fn(&mut Synchronizer) -> Result<Actions, AcquisitionError>;

/// The simulated device: what it is, what it holds, and what it publishes.
/// Every method takes the monotonic nanoseconds its runtime keeps, which here
/// are UNIX nanoseconds, and every packet it sends goes to that runtime's sink.
struct Sim {
    device: Device<'static>,
    settings: Settings,
    streams: [Stream<SEGMENTS>; 3],
    clocks: [Clock; 2],
    capture: CaptureBuffer,
    rng: GaussianRng,
    sync: Synchronizer,
    /// Whether the fake PPS is being fed to the synchronizer.
    pps: bool,
    /// When the next fake pulse lands, on the wall-clock second.
    next_pulse_ns: u64,
    /// The plan the synchronizer last armed, which a start acknowledges.
    plan: Option<u16>,
    acquiring: bool,
    /// What the streams were last told, which a change rolls them over.
    traceable: bool,
    segment_seconds: u32,
    started_ns: u64,
    next_log_at: u64,
    next_log_level: usize,
    no_drop: bool,
    notes: Vec<String>,
}

impl Sim {
    fn new(cli: &SimulateCli, now: u64) -> io::Result<Self> {
        let seed = now ^ u64::from(cli.port).rotate_left(32);
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
        let identity = Identity::new(DEVICE_NAME, DEVICE_DESC, DEVICE_SERIAL, DEVICE_FIRMWARE)
            .ok_or_else(|| invalid_input("identity too long"))?;

        let session = SessionId::new(session_id);
        let mut rng = GaussianRng::new(seed | 1);
        Ok(Self {
            device: Device::new(identity, session, &RPCS),
            settings: Settings::new(cli),
            streams: boot_streams(sample_rate)?,
            clocks,
            capture: CaptureBuffer::new(),
            pps: !cli.no_pps,
            next_pulse_ns: next_second_ns(now),
            plan: None,
            acquiring: false,
            traceable: true,
            sync: synchronizer(session, now),
            segment_seconds: cli.segment_seconds,
            started_ns: now,
            next_log_at: now + next_log_delay(&mut rng),
            next_log_level: 0,
            no_drop: cli.no_drop,
            notes: Vec::new(),
            rng,
        })
    }

    /// A host has connected: the device describes itself. Acquisition is the
    /// device's own business and a reconnecting host does not disturb it.
    fn connected(&mut self, now: u64, out: &mut impl Sink) {
        self.device.connected(&self.streams[..], now, out);
    }

    /// Answer one packet from the host, which for a SYNC packet means taking
    /// its time reference as a parent's. The pulse due this second is fed
    /// first, so an edge beats a packet about the same second.
    fn handle(&mut self, packet: PacketView<'_>, now: u64, out: &mut impl Sink) {
        self.update_capture(now);
        self.advance_sync(now);
        if packet.header.ptype == PacketType::SYNC {
            if let Some(timeref) = sync::Timeref::parse(packet.payload) {
                self.sync
                    .observe_packet(timeref, sync::Timeref::pad(packet.payload), now);
            }
            return;
        }
        let Handled::Rpc(call) = self.device.handle(&self.streams[..], packet, out) else {
            return;
        };
        let result = self.app_rpc(&call, now, out);
        call.reply(result.as_deref().map_err(|error| *error), out);
    }

    /// Send everything due at `now`: the pulse, the heartbeat, a log message,
    /// samples.
    fn tick(&mut self, now: u64, out: &mut impl Sink) {
        self.update_capture(now);
        self.advance_sync(now);
        self.device.tick(now, out);
        self.log_if_due(now, out);
        self.send_due_samples(now, out);
    }

    /// Power-cycle: a new session, the settings a device boots with, a fresh
    /// segment ring on every stream, and a timebase to bootstrap again.
    fn reboot(&mut self, now: u64, out: &mut impl Sink) {
        let session = SessionId::new(self.next_session_id());
        self.device.reboot(session);
        self.settings.reset();
        self.streams =
            boot_streams(self.sample_rate()).expect("streams that booted once boot again");
        self.sync = synchronizer(session, now);
        self.next_pulse_ns = next_second_ns(now);
        self.plan = None;
        self.acquiring = false;
        self.traceable = true;
        self.capture.clear();
        self.next_log_at = now + next_log_delay(&mut self.rng);
        self.next_log_level = 0;
        self.notes.push(format!(
            "rebooted test device; new session id {}",
            self.device.session.value()
        ));
        self.connected(now, out);
    }

    /// Drop one sample from every clock, as the keyboard asks.
    fn drop_now(&mut self) {
        for clock in 0..self.clocks.len() {
            if self.clocks[clock].rolls_over() {
                self.rollover(clock);
            }
            self.drop_sample(clock);
        }
    }

    /// Feed the synchronizer every pulse due by `now`, then the wake it asked
    /// for. Without a pulse the wake is what bootstraps the local timebase,
    /// so it is taken on the second boundary either way.
    fn advance_sync(&mut self, now: u64) {
        while self.next_pulse_ns <= now {
            let at = self.next_pulse_ns;
            self.next_pulse_ns += NANOS_PER_SECOND;
            let actions = match self.pps {
                true => self.sync.capture(PPS_EDGE, at),
                false => self.sync.wake(PPS_EDGE, at),
            };
            self.apply(actions, at);
        }
        if self.sync.status().counter_edge.is_some() && now >= self.sync.next_wake_deadline_ns(now)
        {
            let actions = self.sync.wake(counter_at(now), now);
            self.apply(actions, now);
        }
    }

    /// Carry out one round of synchronization work.
    fn apply(&mut self, actions: Actions, at: u64) {
        match actions.local {
            AcquisitionAction::None => {}
            AcquisitionAction::Arm(plan) | AcquisitionAction::Rearm(plan) => {
                self.plan = Some(plan.id)
            }
            AcquisitionAction::Disarm => self.plan = None,
            AcquisitionAction::Start(edge) => self.start_run(edge, at),
            AcquisitionAction::Stop => self.stop_run(),
        }
        if let Some(status) = actions.status_changed {
            self.follow_traceability(status.traceable);
        }
    }

    /// Begin acquiring at `edge`, whose second every stream's sample zero
    /// sits at.
    fn start_run(&mut self, edge: ScheduledEdge, at: u64) {
        let timeref = timeref_of(edge.reference);
        self.started_ns = at;
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
        self.acquiring = true;
        let plan = self
            .plan
            .take()
            .expect("a start follows the arm that staged it");
        self.sync
            .mark_running(plan)
            .expect("a start is acknowledged out of Starting");
        self.notes
            .push(format!("acquiring from second {}", edge.reference.second));
    }

    fn stop_run(&mut self) {
        for stream in &mut self.streams {
            stream.stop();
        }
        self.acquiring = false;
        self.plan = None;
        self.sync
            .finish_stop()
            .expect("a stop is acknowledged out of Stopping");
        self.notes.push("acquisition stopped".to_string());
    }

    /// A change of traceability ends the segment it happened in, and the one
    /// that opens says whether the pulses were there.
    fn follow_traceability(&mut self, traceable: bool) {
        if self.traceable == traceable {
            return;
        }
        self.traceable = traceable;
        for stream in &mut self.streams {
            stream.set_holdover(!traceable);
            stream.rollover();
        }
    }

    /// One acquisition RPC, answered at once from the state machine and then
    /// carried out.
    fn acquire(&mut self, request: Acquisition, now: u64) -> Result<Reply, RpcError> {
        let actions = request(&mut self.sync).map_err(|_| RpcError::State)?;
        self.apply(actions, now);
        Ok(Reply::new())
    }

    /// Turn the fake pulse train on or off, as the keyboard asks.
    fn toggle_pps(&mut self) {
        self.pps = !self.pps;
        self.notes.push(match self.pps {
            true => "PPS restored".to_string(),
            false => "PPS removed; the device falls into holdover".to_string(),
        });
    }

    /// The RPCs this device adds to the standard ones.
    fn app_rpc(
        &mut self,
        call: &Call<'_, '_>,
        now: u64,
        out: &mut impl Sink,
    ) -> Result<Reply, RpcError> {
        let args = call.args;
        let mut reply = Reply::new();
        match call.name {
            "dev.firmware.upload" => {}
            "dev.start" => return self.acquire(Synchronizer::start, now),
            "dev.stop" => return self.acquire(Synchronizer::stop, now),
            "dev.restart" => return self.acquire(Synchronizer::restart, now),
            "dev.autostart" => {
                let answered = self.device.apply(&mut self.settings.autostart, args, out)?;
                self.sync
                    .set_autostart_seconds(self.settings.autostart.get());
                return Ok(answered);
            }
            "sync.status" => {
                if !args.is_empty() {
                    return Err(RpcError::ReadOnly);
                }
                put(&mut reply, &[self.sync.status().announced.code()])?;
            }
            "dev.firmware.upgrade" => {
                self.device.identity.desc =
                    UPGRADED_DESC.try_into().map_err(|_| RpcError::Internal)?;
            }
            "test.amplitude" => return self.device.apply(&mut self.settings.amplitude, args, out),
            "test.frequency" => return self.device.apply(&mut self.settings.frequency, args, out),
            "test.noise" => return self.device.apply(&mut self.settings.noise, args, out),
            "test.status" => return self.device.apply(&mut self.settings.status, args, out),
            "test.enable" => return self.device.apply(&mut self.settings.enable, args, out),
            "test.go" => {
                if !args.is_empty() {
                    return Err(RpcError::ArgsSize);
                }
                self.notes.push("test.go action invoked".to_string());
            }
            "test.capture" => {
                let selector = Selector::parse(args)?;
                self.capture.view().reply(selector, &mut reply)?;
                if selector == Selector::Trigger {
                    self.trigger_capture(now);
                }
            }
            _ => return Err(RpcError::NotFound),
        }
        Ok(reply)
    }

    fn trigger_capture(&mut self, now: u64) {
        let (data, info) = self.generate_capture_data();
        self.capture
            .begin_capture(data, info, now + CAPTURE_TRIGGER_DELAY_NS);
        self.notes.push(format!(
            "test.capture triggered ({} samples); data available in ~{:.1}s",
            info.length,
            CAPTURE_TRIGGER_DELAY_NS as f64 / 1_000_000_000.0
        ));
    }

    fn update_capture(&mut self, now: u64) {
        let was_locked = self.capture.locked();
        self.capture.update(now);
        if was_locked && !self.capture.locked() {
            self.notes.push(format!(
                "test.capture done ({} bytes)",
                self.capture.export_size()
            ));
        }
    }

    fn generate_capture_data(&mut self) -> (Vec<u8>, CaptureInfo) {
        let sample_count = next_capture_sample_count(&mut self.rng);
        let mut data = Vec::with_capacity(sample_count * CAPTURE_SAMPLE_BYTES);

        let rate = self.sample_rate().get();
        let noise_sigma = self.settings.noise.get() * (f64::from(rate) / 2.0).sqrt();
        let start_sample = self.clocks[WAVE_CLOCK].generated;
        for offset in 0..sample_count as u64 {
            let t = (start_sample + offset) as f64 / f64::from(rate);
            let phase = std::f64::consts::TAU * self.settings.frequency.get() * t;
            let value = self.settings.amplitude.get() * phase.sin()
                + noise_sigma * self.rng.next_gaussian();
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

    fn log_if_due(&mut self, now: u64, out: &mut impl Sink) {
        if now < self.next_log_at {
            return;
        }

        let level = self.next_log_level();
        let lucky_number = (self.rng.next_u64() % 10_000) as u32;
        let message = self.random_log_message(lucky_number);
        self.device.log(level, lucky_number, &message, out);
        self.next_log_at = now + next_log_delay(&mut self.rng);
    }

    fn send_due_samples(&mut self, now: u64, out: &mut impl Sink) {
        if !self.acquiring {
            return;
        }
        (0..self.clocks.len()).for_each(|clock| self.send_clock_samples(clock, now, out));
    }

    /// The samples one clock owes: its streams roll over at a segment
    /// boundary, skip the sample it owes a gap, and publish the rest.
    fn send_clock_samples(&mut self, clock: usize, now: u64, out: &mut impl Sink) {
        for _ in 0..self.clocks[clock].due(now.saturating_sub(self.started_ns)) {
            if self.clocks[clock].rolls_over() {
                self.rollover(clock);
            }
            if self.clocks[clock].next_drop == Some(self.clocks[clock].generated) {
                self.drop_sample(clock);
                continue;
            }
            self.push_samples(clock, out);
            self.clocks[clock].generated += 1;
        }
        self.flush_streams(clock, out);
    }

    /// One sample into each stream the clock drives.
    fn push_samples(&mut self, clock: usize, out: &mut impl Sink) {
        for &signal in self.clocks[clock].streams {
            let sample = self.sample(signal);
            let size = self.streams[signal as usize].def().sample_size();
            self.streams[signal as usize].push(&sample[..size], out);
        }
    }

    fn flush_streams(&mut self, clock: usize, out: &mut impl Sink) {
        for &signal in self.clocks[clock].streams {
            self.streams[signal as usize].flush(out);
        }
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
                sample[..2].copy_from_slice(&[self.settings.status.get(), SIGNAL_LEVEL]);
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
        let phase = std::f64::consts::TAU * self.settings.frequency.get() * t;
        let noise_sigma = self.settings.noise.get() * (rate / 2.0).sqrt();
        let amplitude = self.settings.amplitude.get();
        let sine = amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
        let cosine = amplitude * phase.cos() + noise_sigma * self.rng.next_gaussian();
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
        self.notes.push(format!(
            "dropped sample {} from stream {ids}",
            self.clocks[clock].generated
        ));
        for &signal in self.clocks[clock].streams {
            self.streams[signal as usize].skip(1);
        }
        self.clocks[clock].generated += 1;
        self.clocks[clock].next_drop = self.next_drop(clock);
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

/// What runs the simulated device: the socket it answers on, the client it
/// answers to, the clock it steps with, and the keyboard.
struct Runtime {
    socket: UdpSocket,
    client: Option<Client>,
    sim: Sim,
}

impl Runtime {
    fn new(cli: SimulateCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            client: None,
            sim: Sim::new(&cli, now_ns())?,
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
        self.banner(raw_mode.is_some())?;

        loop {
            if raw_mode.is_some() && !self.handle_keyboard()? {
                terminal_println!("stopping tio test");
                return Ok(());
            }
            self.receive_packets()?;
            self.expire_client();
            self.step(|sim, now, sink| sim.tick(now, sink))?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// What the device is, as the terminal sees it at startup.
    fn banner(&self, keyboard: bool) -> io::Result<()> {
        let port = self.socket.local_addr()?.port();
        let settings = &self.sim.settings;
        terminal_println!("tio test listening on udp://0.0.0.0:{port}");
        terminal_println!(
            "  stream 1: 2 waveform channels, amplitude={} V frequency={} Hz noise={} V/sqrt(Hz) samplerate={} Hz segment={} s",
            settings.amplitude.get(),
            settings.frequency.get(),
            settings.noise.get(),
            self.sim.sample_rate(),
            self.sim.segment_seconds
        );
        terminal_println!(
            "  stream 2: status={} signal_level={}",
            settings.status.get(),
            SIGNAL_LEVEL
        );
        terminal_println!(
            "  stream 3: aux triangle/sawtooth at {} Hz sampled at {} Hz",
            AUX_WAVE_FREQUENCY,
            AUX_SAMPLE_RATE
        );
        if self.sim.no_drop {
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
            CAPTURE_TRIGGER_DELAY_NS as f64 / 1_000_000_000.0
        );
        if keyboard {
            terminal_println!(
                "  press d to drop one sample now, p to toggle the PPS, r to reboot, \
                 Ctrl-C to quit"
            );
        }
        terminal_println!(
            "  {} PPS, acquiring {} s after boot; dev.start, dev.stop, dev.restart",
            if self.sim.pps { "simulated" } else { "no" },
            self.sim.settings.autostart.get()
        );
        terminal_println!("  connect with: tio proxy udp4://127.0.0.1:{port}");
        Ok(())
    }

    /// Step the simulation, with the connected client as its sink.
    fn step(&mut self, act: impl FnOnce(&mut Sim, u64, &mut UdpSink<'_>)) -> io::Result<()> {
        let now = now_ns();
        let mut sink = UdpSink::new(&self.socket, self.client.map(|client| client.addr));
        act(&mut self.sim, now, &mut sink);
        self.sim
            .notes
            .drain(..)
            .for_each(|note| terminal_println!("{note}"));
        sink.finish()
    }

    fn handle_keyboard(&mut self) -> io::Result<bool> {
        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('d') => self.step(|sim, _, _| sim.drop_now())?,
                    KeyCode::Char('p') => self.step(|sim, _, _| sim.toggle_pps())?,
                    KeyCode::Char('r') => self.step(|sim, now, sink| sim.reboot(now, sink))?,
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
                            self.step(|sim, now, sink| sim.handle(packet, now, sink))?;
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
                terminal_println!("client connected: {addr}");
                self.step(|sim, now, sink| sim.connected(now, sink))?;
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
}

/// A child's synchronizer: its own wall clock to start from, a fake PPS to
/// follow, and no oscillator to steer.
fn synchronizer(session: SessionId, now: u64) -> Synchronizer {
    Synchronizer::new(
        CounterDomain::new(PPS_PERIOD),
        PulseConfig::with_edge_tolerance(ticks(PPS_TOLERANCE_NS, PPS_PERIOD)),
        Reference {
            identity: ReferenceIdentity::new(sync::Epoch::UNIX, session, DEVICE_SERIAL.as_bytes()),
            second: (now / NANOS_PER_SECOND) as u32,
        },
        None,
        AUTOSTART_SECONDS,
    )
}

/// Where a segment's sample zero sits, as the synchronizer names it.
fn timeref_of(reference: Reference) -> Timeref {
    let serial = std::str::from_utf8(reference.identity.serial.as_bytes()).unwrap_or(DEVICE_SERIAL);
    Timeref::new(
        reference.identity.epoch,
        reference.second,
        reference.identity.session,
        serial,
    )
    .expect("a timeref serial fits a segment")
}

/// The capture counter at `now`, which the fake pulse divides.
fn counter_at(now: u64) -> u32 {
    (now % NANOS_PER_SECOND * u64::from(PPS_PERIOD) / NANOS_PER_SECOND) as u32
}

/// The wall-clock second after `now`, where the next fake pulse lands.
fn next_second_ns(now: u64) -> u64 {
    (now / NANOS_PER_SECOND + 1) * NANOS_PER_SECOND
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

/// A level the simulated hardware could actually produce.
fn nonnegative(value: f64) -> Result<f64, RpcError> {
    (value.is_finite() && value >= 0.0)
        .then_some(value)
        .ok_or(RpcError::Invalid)
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_string())
}

fn next_log_delay(rng: &mut GaussianRng) -> u64 {
    LOG_MESSAGE_MIN_INTERVAL_NS + (LOG_MESSAGE_JITTER_NS as f64 * rng.next_unit()) as u64
}

fn next_capture_sample_count(rng: &mut GaussianRng) -> usize {
    let span = CAPTURE_SAMPLE_COUNT_MAX - CAPTURE_SAMPLE_COUNT_MIN + 1;
    CAPTURE_SAMPLE_COUNT_MIN + (rng.next_u64() as usize % span)
}

/// The device's clock, which here is the wall clock in nanoseconds.
fn now_ns() -> u64 {
    unix_duration().as_nanos() as u64
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
    use twinleaf::proto::packet::{Packet, PacketType};
    use twinleaf::proto::rpc::Answer;
    use twinleaf::proto::settings::Setting as Announcement;
    use twinleaf::proto::RpcRequestId;
    use twinleaf_device::metadata::{self, Streams};
    use twinleaf_device::rpc::REPLY_MAX;
    use twinleaf_device::sync::{ReferenceState, TimeStatus};

    /// A round wall-clock second to boot the simulated device at.
    const BOOT: u64 = 1_800_000_000 * NANOS_PER_SECOND;
    const PARENT_SERIAL: &str = "PARENT01";
    const PARENT_SESSION: u32 = 4242;
    /// Seconds between the parent's timeline and the simulator's own.
    const PARENT_OFFSET: u32 = 1_000;

    /// Every packet the simulation sent.
    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    impl Sent {
        fn views(&self) -> Vec<PacketView<'_>> {
            self.0
                .iter()
                .map(|packet| PacketView::parse_prefix(packet).unwrap().0)
                .collect()
        }
    }

    /// The segment the sine stream is acquiring.
    fn sine(sim: &Sim) -> &twinleaf_device::segments::Segment {
        sim.streams[Signal::Sine as usize].current()
    }

    /// What the sine stream's current segment says about itself on the wire.
    fn sine_flags(sim: &Sim) -> data::SegmentFlags {
        sim.streams[Signal::Sine as usize]
            .segment(CURRENT_SEGMENT)
            .expect("a current segment")
            .flags
    }

    /// A device at the instant it booted.
    fn sim(args: &[&str]) -> Sim {
        let cli = SimulateCli::parse_from([&["tio-simulate", "--port", "0"], args].concat());
        Sim::new(&cli, BOOT).unwrap()
    }

    /// A device at the second its autostart began acquiring, having locked
    /// to its PPS first.
    fn acquiring(args: &[&str]) -> (Sim, u64) {
        let mut sim = sim(args);
        let mut sent = Sent::default();
        let started = (1..=10u64)
            .map(|second| BOOT + second * NANOS_PER_SECOND)
            .find(|&now| {
                sim.tick(now, &mut sent);
                sim.acquiring
            })
            .expect("the simulator never autostarted");
        (sim, started)
    }

    /// The reply the simulation answered an RPC with.
    fn call(sim: &mut Sim, name: &[u8], args: &[u8], sent: &mut Sent) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let method = twinleaf::proto::rpc::Method::ByName(name);
        let len = twinleaf::proto::rpc::write_request(&mut buf, RpcRequestId::new(1), method, args)
            .unwrap();
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        sim.handle(view, BOOT, sent);
        let view = *sent.views().last().expect("a reply");
        match Answer::parse(view.header.ptype, view.payload) {
            Some(Answer::Reply(reply)) => reply.value.to_vec(),
            Some(Answer::Error(error)) => panic!("{:?}", error.error()),
            None => panic!("an answer"),
        }
    }

    #[test]
    fn the_table_describes_the_settings_it_answers() {
        let sim = sim(&[]);
        let settings = &sim.settings;
        let specs = [
            settings.amplitude.spec(),
            settings.frequency.spec(),
            settings.noise.spec(),
            settings.status.spec(),
            settings.enable.spec(),
        ];
        let listed: Vec<_> = RPCS
            .iter()
            .filter(|spec| specs.iter().any(|derived| derived.name == spec.name))
            .cloned()
            .collect();
        assert_eq!(listed, specs);
    }

    #[test]
    fn a_setting_write_is_announced_and_counted() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();

        let value = 2.5f64.to_le_bytes();
        assert_eq!(call(&mut sim, b"test.amplitude", &value, &mut sent), value);
        assert_eq!(sim.settings.amplitude.get(), 2.5);

        let announcements: Vec<_> = sent
            .views()
            .iter()
            .filter(|view| view.header.ptype == PacketType::SETTING)
            .map(|view| Announcement::parse(view.payload).unwrap())
            .map(|setting| (setting.name.to_vec(), setting.reply.to_vec()))
            .collect();
        assert_eq!(
            announcements,
            [(b"test.amplitude".to_vec(), value.to_vec())]
        );

        let mut sent = Sent::default();
        let version = call(&mut sim, b"settings.version", &[], &mut sent);
        assert_eq!(version, 1u32.to_le_bytes());
    }

    #[test]
    fn a_reboot_restores_the_settings_a_boot_starts_from() {
        let mut sim = sim(&["--amplitude", "1"]);
        let mut sent = Sent::default();
        call(
            &mut sim,
            b"test.amplitude",
            &7.5f64.to_le_bytes(),
            &mut sent,
        );
        call(&mut sim, b"test.enable", &[0], &mut sent);

        sim.reboot(BOOT, &mut sent);
        assert_eq!(sim.settings.amplitude.get(), 1.0);
        assert!(sim.settings.enable.get());

        let mut sent = Sent::default();
        let version = call(&mut sim, b"settings.version", &[], &mut sent);
        assert_eq!(version, 0u32.to_le_bytes());
    }

    #[test]
    fn capture_buffer_exports_indexed_blocks_after_delay() {
        let mut capture = CaptureBuffer::new();
        capture.block_size = 4;
        capture.begin_capture(
            (0u8..10).collect(),
            CaptureInfo {
                length: 10,
                ..CaptureInfo::default()
            },
            500,
        );

        assert!(capture.locked());
        assert_eq!(capture.status(), capture::Status::Capturing);

        capture.update(499);
        assert!(capture.locked());

        capture.update(500);
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
        let sim = sim(&[]);
        let streams = &sim.streams[..];
        let mut out = Reply::new();
        metadata::reply(sim.device.record(streams), streams, &[], &mut out).unwrap();
        let kinds: Vec<_> = data::MetadataReply::parse(&out)
            .unwrap()
            .map(|(kind, _)| kind)
            .collect();
        assert_eq!(kinds[0], data::MetadataType::Device);
        assert_eq!(kinds.len(), 1 + sim.streams.len() * 4);
    }

    #[test]
    fn metadata_reports_the_ring_and_the_current_segment() {
        let (mut sim, now) = acquiring(&["--samplerate", "4", "--segment-seconds", "10"]);
        sim.tick(now + NANOS_PER_SECOND, &mut Sent::default());

        let streams = &sim.streams[..];
        let record = streams.stream(1).unwrap();
        assert_eq!(record.n_segments, SEGMENTS as u8);
        assert_eq!(record.buf_samples, 0);
        assert_eq!(record.sample_size, 16);
        let segment = streams.segment(1, CURRENT_SEGMENT).unwrap();
        assert_eq!(segment.sampling_rate, 4);
        assert_eq!(segment.filter_cutoff, 2.0);
        assert_eq!(segment.epoch, sync::Epoch::UNIX);
        assert_eq!(segment.timeref_serial, DEVICE_SERIAL);
        assert_eq!(
            segment.flags,
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE
        );
        assert_eq!(
            streams.segment(1, segment.segment_id.value()),
            Some(segment)
        );
        assert!(streams.stream(99).is_none());
        assert!(streams.segment(99, CURRENT_SEGMENT).is_none());
    }

    #[test]
    fn a_clock_reaching_its_segment_length_rolls_over() {
        let (mut sim, _) = acquiring(&["--samplerate", "4", "--segment-seconds", "1"]);
        let mut sent = Sent::default();
        let start_time = sine(&sim).timeref().start_time;
        assert_eq!(sim.clocks[WAVE_CLOCK].generated, 0);
        for _ in 0..=sim.clocks[WAVE_CLOCK].segment_samples {
            if sim.clocks[WAVE_CLOCK].rolls_over() {
                sim.rollover(WAVE_CLOCK);
            }
            sim.push_samples(WAVE_CLOCK, &mut sent);
            sim.clocks[WAVE_CLOCK].generated += 1;
        }

        assert_eq!(sine(&sim).id().value(), 1);
        assert_eq!(sine(&sim).timeref().start_time, start_time + 1);
    }

    /// A reconnecting host is not a reboot: it disturbs neither the run nor
    /// the segment it is in.
    #[test]
    fn a_reconnect_leaves_the_run_alone_and_a_reboot_starts_a_fresh_ring() {
        let (mut sim, now) = acquiring(&["--samplerate", "4", "--segment-seconds", "1"]);
        let mut sent = Sent::default();
        let segment = sine(&sim).id().value();
        let start_time = sine(&sim).timeref().start_time;

        sim.connected(now, &mut sent);
        assert!(sim.acquiring);
        assert_eq!(sine(&sim).id().value(), segment);
        assert_eq!(sine(&sim).timeref().start_time, start_time);

        let session = sim.device.session;
        sim.reboot(now, &mut sent);
        assert_ne!(sim.device.session, session);
        assert!(!sim.acquiring);
        assert!(sim
            .streams
            .iter()
            .all(|stream| stream.current().id().value() == 0));
    }

    /// The simulator in the child role: a parent's SYNC packets take it onto
    /// the parent's timeline, and the segment that opens says so.
    #[test]
    fn a_parents_sync_packets_are_adopted_and_name_the_new_segment() {
        let (mut sim, now) = acquiring(&["--samplerate", "4"]);
        let mut sent = Sent::default();
        let mut at = now;
        for sequence in 1..=6u8 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
            let timeref = sync::Timeref {
                epoch: sync::Epoch::UNIX,
                time: (at / NANOS_PER_SECOND) as u32 + PARENT_OFFSET,
                session: SessionId::new(PARENT_SESSION),
                serial: PARENT_SERIAL.as_bytes(),
            };
            let mut buf = [0u8; 64];
            let len = timeref
                .write_with_pad(&mut buf, TimeStatus::Locked.bits() | sequence)
                .unwrap();
            let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
            sim.handle(view, at, &mut sent);
        }
        assert_eq!(sim.sync.status().reference, ReferenceState::Upstream);
        assert_eq!(sim.sync.status().upstream, TimeStatus::Locked);

        // Adoption restarts the run, which then begins on the parent's time.
        for _ in 0..4 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
        }
        assert!(sim.acquiring);
        assert_eq!(sine(&sim).timeref().serial, PARENT_SERIAL);
        assert_eq!(sine(&sim).timeref().session, SessionId::new(PARENT_SESSION));
        assert!(sine(&sim).timeref().start_time > PARENT_OFFSET);
        assert!(sim.traceable);
    }

    /// The 'p' key's demonstration: the pulses go away, the segment ends, and
    /// the one that opens carries the holdover flag.
    #[test]
    fn losing_the_pulses_rolls_over_and_flags_the_segment() {
        let (mut sim, now) = acquiring(&["--samplerate", "4"]);
        let mut sent = Sent::default();
        let mut at = now + NANOS_PER_SECOND;
        sim.tick(at, &mut sent);
        assert!(sim.traceable);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE
        );

        sim.toggle_pps();
        for _ in 0..4 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
        }
        assert!(!sim.traceable);
        assert_eq!(sim.sync.status().announced, TimeStatus::Holdover);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE | data::SegmentFlags::HOLDOVER
        );
        assert_eq!(sine(&sim).id().value(), 1);
    }

    /// D8: a device whose pulses never arrived says so in the segments it
    /// opens, not only in the ones a later change rolls over to.
    #[test]
    fn a_device_that_never_had_pulses_flags_its_first_segment() {
        let (mut sim, now) = acquiring(&["--samplerate", "4", "--no-pps"]);
        sim.tick(now + NANOS_PER_SECOND, &mut Sent::default());
        assert!(!sim.traceable);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE | data::SegmentFlags::HOLDOVER
        );
    }

    /// D9: every acquisition RPC is answered from the state machine at once,
    /// and a stop from idle is not an error.
    #[test]
    fn the_acquisition_rpcs_answer_at_once_and_take_effect() {
        let (mut sim, now) = acquiring(&["--samplerate", "4"]);
        let mut sent = Sent::default();
        let mut at = now + NANOS_PER_SECOND;
        sim.tick(at, &mut sent);

        assert!(call(&mut sim, b"dev.stop", &[], &mut sent).is_empty());
        assert!(!sim.acquiring);
        assert!(call(&mut sim, b"dev.stop", &[], &mut sent).is_empty());

        call(&mut sim, b"dev.start", &[], &mut sent);
        for _ in 0..3 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
        }
        assert!(sim.acquiring);

        call(&mut sim, b"dev.restart", &[], &mut sent);
        assert!(!sim.acquiring);
        for _ in 0..3 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
        }
        assert!(sim.acquiring);

        assert_eq!(
            call(&mut sim, b"sync.status", &[], &mut sent),
            [TimeStatus::Locked.code()]
        );
        assert_eq!(call(&mut sim, b"dev.autostart", &[5], &mut sent), [5]);
        assert_eq!(sim.sync.autostart_seconds(), 5);
    }

    #[test]
    fn capture_data_uses_current_sine_parameters() {
        let mut sim = sim(&[
            "--samplerate",
            "4",
            "--frequency",
            "1",
            "--amplitude",
            "2",
            "--noise",
            "0",
        ]);

        let (data, info) = sim.generate_capture_data();

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
        let mut sim = sim(&[]);
        let (data, info) = sim.generate_capture_data();
        let data_len = data.len();
        sim.capture.begin_capture(data, info, 1);
        sim.capture.update(1);

        let mut out = Reply::new();
        sim.capture
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
    fn a_log_message_is_due_on_its_own_schedule() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        sim.log_if_due(sim.next_log_at - 1, &mut sent);
        assert!(sent.0.is_empty());

        sim.log_if_due(sim.next_log_at, &mut sent);
        let [view] = sent.views()[..] else {
            panic!("one packet");
        };
        assert_eq!(view.header.ptype, PacketType::LOG);
        let entry = log::LogMessage::parse(view.payload).unwrap();
        assert_eq!(entry.level, log::LogLevel::CRITICAL);
        let lucky = format!("lucky number {}", entry.data);
        assert!(std::str::from_utf8(entry.message).unwrap().contains(&lucky));
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

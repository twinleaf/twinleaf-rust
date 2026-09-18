//! tio simulate
//!
//! Simulates a small Twinleaf device that publishes a noisy sine wave on
//! stream 1, or, with `--children`, a hub and the tree of them below it.

use crate::SimulateCli;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::StandardNormal;
use ratatui::crossterm::{
    event::{self, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::collections::hash_map::RandomState;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::proto::capture::{CaptureMetadata, METADATA_VERSION};
use twinleaf::proto::packet::{PacketType, PacketView};
use twinleaf::proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf::proto::{data, log, sync};
use twinleaf::proto::{BoardId, FirmwareMagic, HwRev, SessionId, StreamId};
use twinleaf_device::data::{filter, ColumnDef, Params, Stream, StreamDef, Timeref};
use twinleaf_device::device::{
    self, Deferred, Device, Entry, FlashOp, Group, Identity, OneLane, SyncRequest,
};
use twinleaf_device::hub::{CallError, Event, Events, Hub, Input, PortSink};
use twinleaf_device::rpc::{
    put, Access, Capture, Kind, Reply, RpcSpec, Selector, Setting, Status, STANDARD,
};
use twinleaf_device::storage::update::{Package, Take, Upload};
use twinleaf_device::sync::{
    ticks, AcquisitionAction, AcquisitionError, Actions, Announce, CounterDomain, PulseConfig,
    Reference, ReferenceIdentity, ScheduledEdge, Synchronizer, TimeStatus,
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
const DEVICE_MCU: &str = "simulated";
const HUB_NAME: &str = "tio-hub";
const HUB_DESC: &str = "Twinleaf tio-hub R1 ((null)) [2026-06-08/000001-DEV]";
const HUB_SERIAL: &str = "HUB-SIM";
/// Hops a hub recognizes: port `n` is the route `/n`, and the children hang
/// off `/1` to `/9` as a proxy's mounts do, which leaves port 0 empty.
const MAX_PORTS: usize = 10;
/// How long a cable takes to carry the hub's pulse to a child.
const PPS_DELAY_NS: u64 = 800;
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

/// The RPC table: the standard entries every platform declares, then the
/// simulator's own.
static RPCS: LazyLock<Vec<RpcSpec>> = LazyLock::new(|| {
    STANDARD
        .iter()
        .cloned()
        .chain([
            RpcSpec::prop("dev.autostart", Kind::Uint(1), Access::RW),
            RpcSpec::prop(
                "test.amplitude",
                Kind::Float(8),
                Access::RW.union(Access::PERSISTENT),
            ),
            RpcSpec::prop(
                "test.frequency",
                Kind::Float(8),
                Access::RW.union(Access::PERSISTENT),
            ),
            RpcSpec::prop(
                "test.noise",
                Kind::Float(8),
                Access::RW.union(Access::PERSISTENT),
            ),
            RpcSpec::prop("test.status", Kind::Uint(1), Access::RW),
            RpcSpec::prop("test.enable", Kind::Bool, Access::RW),
            RpcSpec::action("test.go"),
            RpcSpec::std("test.capture", Access::READ)
                .with_extra_meta(RpcMetaFlags::READABLE.union(RpcMetaFlags::CAPTURE).bits()),
        ])
        .collect()
});

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
    fn new(cli: &SimulateCli, autostart: u8) -> Self {
        Self {
            amplitude: Setting::new("test.amplitude", cli.amplitude)
                .checked(nonnegative)
                .persistent(),
            frequency: Setting::new("test.frequency", cli.frequency)
                .checked(nonnegative)
                .persistent(),
            noise: Setting::new("test.noise", cli.noise)
                .checked(nonnegative)
                .persistent(),
            status: Setting::new("test.status", 0),
            enable: Setting::new("test.enable", true),
            autostart: Setting::new("dev.autostart", autostart),
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

/// The order the table lists them in, which is the order their ids follow the
/// standard entries'.
impl Group for Settings {
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
        Group::entries(&mut self.autostart, visit);
        Group::entries(&mut self.amplitude, visit);
        Group::entries(&mut self.frequency, visit);
        Group::entries(&mut self.noise, visit);
        Group::entries(&mut self.status, visit);
        Group::entries(&mut self.enable, visit);
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

    fn status(&self) -> Status {
        if self.capturing.is_some() {
            Status::Capturing
        } else if self.data.is_empty() {
            Status::Idle
        } else {
            Status::Done
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

/// A generator seeded from the operating system, so every device and run differs.
fn seeded() -> SmallRng {
    SmallRng::seed_from_u64(RandomState::new().build_hasher().finish())
}

fn gaussian(rng: &mut SmallRng) -> f64 {
    rng.sample(StandardNormal)
}

/// One acquisition RPC's effect on the synchronizer.
type Acquisition = fn(&mut Synchronizer) -> Result<Actions, AcquisitionError>;

/// What a simulated device is in the tree.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A device with the test streams, alone or on a child port.
    Sensor,
    /// The root of a tree: no streams, and the time its children follow.
    Hub,
}

/// Where a device's second edges come from.
#[derive(Clone, Copy)]
enum Pulses {
    /// Its own simulated pulse, on each wall-clock second.
    Own {
        /// When the next one lands.
        next_ns: u64,
    },
    /// None at all: each second is closed by a wake instead.
    None {
        /// When the next second closes.
        next_ns: u64,
    },
    /// The hub's, down a cable.
    Cable,
}

impl Pulses {
    /// A device's own pulse train, present or not.
    fn own(present: bool, now: u64) -> Self {
        let next_ns = next_second_ns(now);
        match present {
            true => Self::Own { next_ns },
            false => Self::None { next_ns },
        }
    }

    /// The next self-made second boundary at or before `now`, taking it.
    fn next(&mut self, now: u64) -> Option<u64> {
        match self {
            Self::Cable => None,
            Self::Own { next_ns } | Self::None { next_ns } if *next_ns <= now => {
                let at = *next_ns;
                *next_ns += NANOS_PER_SECOND;
                Some(at)
            }
            Self::Own { .. } | Self::None { .. } => None,
        }
    }

    /// Whether those boundaries carry a pulse.
    fn pulsing(&self) -> bool {
        matches!(self, Self::Own { .. })
    }

    /// The same source, restarted at the second after `now`.
    fn restarted(&self, now: u64) -> Self {
        match self {
            Self::Cable => Self::Cable,
            Self::Own { .. } => Self::own(true, now),
            Self::None { .. } => Self::own(false, now),
        }
    }
}

/// The package the simulator takes, which is the one `just package sandi`
/// signs, so `tio upgrade` against the simulator sends what a Sandi is sent.
const PACKAGE: Package = Package {
    magic: FirmwareMagic::new(*b"SANDIFW\0"),
    board_id: BoardId::from_ascii("COMM-USB"),
    hw_rev: HwRev::new(8),
    development: true,
    image_max: 224 * 1024,
};

/// The platform's flash: the firmware image an upload writes, whether one has
/// taken, and the configuration a save left behind.
#[derive(Default)]
struct Flash {
    image: Vec<u8>,
    upgraded: bool,
    conf: Option<Vec<u8>>,
}

/// The simulated device: what it is, what it holds, and what it publishes.
/// Every method takes the monotonic nanoseconds its runtime keeps, which here
/// are UNIX nanoseconds, and every packet it sends goes to that runtime's sink.
struct Sim {
    role: Role,
    device: Device<Settings>,
    streams: Vec<Stream<SEGMENTS>>,
    clocks: Vec<Clock>,
    capture: CaptureBuffer,
    /// What the platform keeps across a reboot.
    flash: Flash,
    /// How much of a firmware image `dev.firmware.upload` has taken.
    upload: Upload,
    rng: SmallRng,
    sync: Synchronizer,
    /// Where its second edges come from.
    pulses: Pulses,
    /// The second the synchronizer just closed, for whoever passes it on.
    announced: Option<(u64, Announce)>,
    /// The plan the synchronizer last armed, which a start acknowledges.
    plan: Option<u16>,
    acquiring: bool,
    /// What the streams were last told, which a change rolls them over.
    holdover: bool,
    rate: NonZeroU32,
    segment_seconds: u32,
    started_ns: u64,
    next_log_at: u64,
    next_log_level: usize,
    no_drop: bool,
    notes: Vec<String>,
}

impl Sim {
    /// The device at `port`, with 0 the root, as `role` has it.
    fn new(cli: &SimulateCli, role: Role, port: u8, pulses: Pulses, now: u64) -> io::Result<Self> {
        let mut rng = seeded();
        let session_id: u32 = rng.random();
        let rate = NonZeroU32::new(cli.samplerate)
            .ok_or_else(|| invalid_input("sample rate must be at least one hertz"))?;
        let serial = serial_of(role, port);
        let identity = identity(role, &serial)?;

        let session = SessionId::new(session_id);
        Ok(Self {
            role,
            device: Device::new(
                identity,
                session,
                &RPCS,
                Settings::new(cli, autostart_of(role)),
                &cli.password,
                now,
            ),
            streams: boot_streams(role, rate)?,
            clocks: boot_clocks(role, rate, cli.segment_seconds)?,
            capture: CaptureBuffer::new(),
            flash: Flash::default(),
            upload: Upload::new(),
            pulses,
            announced: None,
            plan: None,
            acquiring: false,
            holdover: false,
            sync: synchronizer(role, session, &serial_of(role, port), now),
            rate,
            segment_seconds: cli.segment_seconds,
            started_ns: now,
            next_log_at: now + next_log_delay(&mut rng),
            next_log_level: 0,
            no_drop: cli.no_drop,
            notes: Vec::new(),
            rng,
        })
    }

    /// One pulse arriving from outside, down the cable from a hub.
    fn pulse(&mut self, edge: u32, at: u64) {
        let actions = self.sync.capture(edge, at);
        self.apply(actions, at);
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
        let device::Actions {
            call,
            deferred,
            reboot,
            loglevel: _,
            log,
        } = self.device.handle(
            &self.streams[..],
            device::Input::Packet(packet),
            now,
            &mut OneLane(&mut *out),
        );
        self.note(log);
        if let Some((pending, name, args)) = call {
            let result = self.app_rpc(name, args, now);
            device::answer(pending, result.as_deref().map_err(|error| *error), out);
        }
        if let Some((pending, work)) = deferred {
            self.serve(Some(pending), work, now, out);
        }
        if reboot {
            self.reboot(now, out);
        }
        self.sync
            .set_autostart_seconds(self.device.settings().autostart.get());
    }

    /// Carry out an RPC whose answer is not the control machine's, and answer
    /// it where the request that asked says.
    fn serve(
        &mut self,
        pending: Option<device::Pending<'_>>,
        work: Deferred,
        now: u64,
        out: &mut impl Sink,
    ) {
        let result = match work {
            Deferred::Acquire(SyncRequest::Start) => self.acquire(Synchronizer::start, now),
            Deferred::Acquire(SyncRequest::Stop) => self.acquire(Synchronizer::stop, now),
            Deferred::Acquire(SyncRequest::Restart) => self.acquire(Synchronizer::restart, now),
            Deferred::Flash(FlashOp::Upload(chunk)) => self.take_chunk(&chunk, now),
            Deferred::Flash(FlashOp::Upgrade) => self.commit_image(now),
            Deferred::Flash(FlashOp::Abort) => self.upload.abort().map(|()| Reply::new()),
            Deferred::Flash(FlashOp::ConfSave(image)) => {
                self.flash.conf = Some(image.to_vec());
                self.notes.push(format!(
                    "configuration saved ({} bytes to flash)",
                    image.len()
                ));
                Ok(Reply::new())
            }
            Deferred::Flash(FlashOp::ConfReset) => {
                self.flash.conf = None;
                self.notes
                    .push("stored configuration cleared; rebooting on the defaults".to_string());
                self.reboot(now, out);
                Ok(Reply::new())
            }
            Deferred::Flash(FlashOp::ConfLoad) => return self.restore(pending, now, out),
        };
        if let Some(pending) = pending {
            device::answer(pending, result.as_deref().map_err(|error| *error), out);
        }
    }

    /// Hand the control machine what is in flash, for the `dev.conf.load` that
    /// asked for it or for the boot that restores unasked.
    fn restore(&mut self, pending: Option<device::Pending<'_>>, now: u64, out: &mut impl Sink) {
        let stored = self.flash.conf.as_deref().ok_or(RpcError::State);
        let actions = self.device.handle(
            &self.streams[..],
            device::Input::Loaded(pending, stored),
            now,
            &mut OneLane(out),
        );
        self.note(actions.log);
    }

    /// Say on the terminal what the control machine had to say.
    fn note(&mut self, log: Option<(log::LogLevel, &'static str)>) {
        self.notes
            .extend(log.map(|(_, message)| message.to_string()));
    }

    /// Send everything due at `now`: the pulse, the heartbeat, a log message,
    /// samples.
    fn tick(&mut self, now: u64, out: &mut impl Sink) {
        self.update_capture(now);
        self.advance_sync(now);
        if let Some(image) = self.device.tick(now, out) {
            self.serve(None, Deferred::Flash(FlashOp::ConfSave(image)), now, out);
            self.device.saved();
        }
        self.log_if_due(now, out);
        self.send_due_samples(now, out);
    }

    /// Power-cycle: a new session, the settings a device boots with, a fresh
    /// segment ring on every stream, and a timebase to bootstrap again.
    fn reboot(&mut self, now: u64, out: &mut impl Sink) {
        let session = SessionId::new(self.next_session_id());
        let serial = self.device.identity.serial.clone();
        self.device.reboot(session, now);
        self.device.settings_mut().reset();
        self.upload = Upload::new();
        self.take_desc();
        self.streams =
            boot_streams(self.role, self.rate).expect("streams that booted once boot again");
        self.sync = synchronizer(self.role, session, &serial, now);
        self.pulses = self.pulses.restarted(now);
        self.announced = None;
        self.plan = None;
        self.acquiring = false;
        self.holdover = false;
        self.capture.clear();
        self.next_log_at = now + next_log_delay(&mut self.rng);
        self.next_log_level = 0;
        self.notes.push(format!(
            "rebooted; new session id {}",
            self.device.session.value()
        ));
        self.connected(now, out);
        self.restore(None, now, out);
    }

    /// Take `dev.desc` from what is in flash: the upgraded build once an
    /// image has taken.
    fn take_desc(&mut self) {
        self.device.identity.desc = desc_of(self.role, self.flash.upgraded)
            .try_into()
            .expect("a description that fits");
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
        while let Some(at) = self.pulses.next(now) {
            let actions = match self.pulses.pulsing() {
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
        self.device
            .set_time_status(self.sync.status().announced.code());
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
            self.follow_holdover(status.announced == TimeStatus::Holdover);
        }
        if let Some(announce) = actions.announce {
            self.announced = Some((at, announce));
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

    /// A change of holdover ends the segment it happened in, and the one that
    /// opens says whether the pulses that named its seconds were there.
    fn follow_holdover(&mut self, holdover: bool) {
        if self.holdover == holdover {
            return;
        }
        self.holdover = holdover;
        for stream in &mut self.streams {
            stream.set_holdover(holdover);
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

    /// Turn the simulated pulse train on or off, as the keyboard asks.
    fn toggle_pps(&mut self) {
        self.pulses = match self.pulses {
            Pulses::Own { next_ns } => Pulses::None { next_ns },
            Pulses::None { next_ns } => Pulses::Own { next_ns },
            Pulses::Cable => Pulses::Cable,
        };
        self.notes.push(match self.pulses.pulsing() {
            true => "PPS restored".to_string(),
            false => "PPS removed; the device falls into holdover".to_string(),
        });
    }

    /// The RPCs this device answers that no table entry does.
    fn app_rpc(&mut self, name: &str, args: &[u8], now: u64) -> Result<Reply, RpcError> {
        let mut reply = Reply::new();
        match name {
            "test.go" => self.notes.push("test.go action invoked".to_string()),
            "test.capture" => {
                let selector = Selector::parse(args)?;
                self.capture.view().reply(selector, &mut reply)?;
                if selector == Selector::Trigger {
                    self.trigger_capture(now);
                }
            }
            _ => return Err(RpcError::State),
        }
        Ok(reply)
    }

    /// One chunk of an update package at the cursor, or a read of the cursor.
    fn take_chunk(&mut self, chunk: &[u8], now: u64) -> Result<Reply, RpcError> {
        if !chunk.is_empty() {
            match self.upload.chunk(&PACKAGE, chunk, now)? {
                Take::Header { discarded: true } => self.flash.image.clear(),
                Take::Header { discarded: false } => {}
                Take::Image(image) => {
                    self.flash.image.truncate(image.offset() as usize);
                    self.flash.image.extend_from_slice(image.bytes());
                    image.written();
                }
            }
        }
        let mut reply = Reply::new();
        put(&mut reply, &self.upload.cursor(now).to_le_bytes())?;
        Ok(reply)
    }

    /// Take the uploaded image, which then survives a reboot. The simulator
    /// holds no key, so the manifest's signature is taken on trust.
    fn commit_image(&mut self, now: u64) -> Result<Reply, RpcError> {
        self.upload.upgrade(now)?;
        self.upload.armed();
        self.flash.upgraded = true;
        self.take_desc();
        self.notes
            .push("firmware image committed; it survives a reboot".to_string());
        Ok(Reply::new())
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
        let noise_sigma = self.device.settings().noise.get() * (f64::from(rate) / 2.0).sqrt();
        let start_sample = self.clocks[WAVE_CLOCK].generated;
        for offset in 0..sample_count as u64 {
            let t = (start_sample + offset) as f64 / f64::from(rate);
            let phase = std::f64::consts::TAU * self.device.settings().frequency.get() * t;
            let value = self.device.settings().amplitude.get() * phase.sin()
                + noise_sigma * gaussian(&mut self.rng);
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
        let lucky_number: u32 = self.rng.random_range(0..10_000);
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
                sample[..2].copy_from_slice(&[self.device.settings().status.get(), SIGNAL_LEVEL]);
                sample
            }
            Signal::Aux => self.aux_sample(),
        }
    }

    /// The rate of the streams the wave clock drives.
    fn sample_rate(&self) -> NonZeroU32 {
        self.rate
    }

    fn sine_sample(&mut self) -> [u8; 16] {
        let rate = f64::from(self.sample_rate().get());
        let t = self.clocks[WAVE_CLOCK].generated as f64 / rate;
        let phase = std::f64::consts::TAU * self.device.settings().frequency.get() * t;
        let noise_sigma = self.device.settings().noise.get() * (rate / 2.0).sqrt();
        let amplitude = self.device.settings().amplitude.get();
        let sine = amplitude * phase.sin() + noise_sigma * gaussian(&mut self.rng);
        let cosine = amplitude * phase.cos() + noise_sigma * gaussian(&mut self.rng);
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
            + self.rng.random::<f64>() * SAMPLE_DROP_JITTER_SECONDS * 2.0;
        let interval = (seconds * f64::from(rate)).round().max(1.0) as u64;
        Some(generated.saturating_add(interval))
    }

    fn next_session_id(&mut self) -> u32 {
        let mut session_id: u32 = self.rng.random();
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
        let template = templates[self.rng.random_range(0..templates.len())];
        template.replace("{lucky}", &lucky_number.to_string())
    }
}

/// Whether a child's cable is connected.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Plug {
    In,
    Out,
}

/// The wire to one child: how late the hub's time arrives, how much of it is
/// lost, and how far the child's own counter walks away from it.
#[derive(Clone, Copy)]
struct Cable {
    pps_delay_ns: u64,
    sync_latency_ns: u64,
    sync_drop: u8,
    pps_present: bool,
    drift_ppm: f64,
}

/// One child device and the cable it hangs off.
struct Child {
    sim: Sim,
    cable: Cable,
    plug: Plug,
    /// The time references on the cable, each due at the instant it lands: a
    /// wire delays what it carries, it does not throw it away.
    inflight: VecDeque<(u64, Vec<u8>)>,
    booted_ns: u64,
}

impl Child {
    fn new(cli: &SimulateCli, port: u8, cable: Cable, now: u64) -> io::Result<Self> {
        let pulses = match cable.pps_present {
            true => Pulses::Cable,
            false => Pulses::own(false, now),
        };
        Ok(Self {
            sim: Sim::new(cli, Role::Sensor, port, pulses, now)?,
            cable,
            plug: Plug::In,
            inflight: VecDeque::new(),
            booted_ns: now,
        })
    }

    /// The counter value this child reads when the hub's pulse reaches it: the
    /// cable's delay, and however far its own counter has walked since boot.
    fn edge_at(&self, at: u64) -> u32 {
        let drift = at.saturating_sub(self.booted_ns) as f64 * self.cable.drift_ppm / 1e6;
        let delay = self.cable.pps_delay_ns as i64 + drift as i64;
        counter_at(delay.rem_euclid(NANOS_PER_SECOND as i64) as u64)
    }
}

/// Packets on their way up a child's cable, each with room for the hop the
/// hub writes on it.
#[derive(Default)]
struct Wire(Vec<Vec<u8>>);

impl Sink for Wire {
    fn send(&mut self, packet: &[u8]) {
        self.0.push(packet.iter().copied().chain([0]).collect());
    }
}

/// Packets the hub is sending down to its children.
#[derive(Default)]
struct Downlink(Vec<(u8, Vec<u8>)>);

impl PortSink for Downlink {
    fn send(&mut self, port: u8, packet: &[u8]) {
        self.0.push((port, packet.to_vec()));
    }
}

/// What the hub asks a child on its own behalf.
#[derive(Clone, Copy)]
enum Ask {
    Name(u8),
}

/// What the hub noticed, as the tree keeps it.
enum Noticed {
    Plugged(u8),
    Unplugged(u8),
    Named(u8, Result<String, RpcError>),
}

#[derive(Default)]
struct Seen(Vec<Noticed>);

impl Events<Ask> for Seen {
    fn event(&mut self, event: Event<'_, Ask>) {
        self.0.push(match event {
            Event::Plugged(port) => Noticed::Plugged(port),
            Event::Unplugged(port) => Noticed::Unplugged(port),
            Event::Answered(Ask::Name(port), answer) => Noticed::Named(
                port,
                answer.map(|value| String::from_utf8_lossy(value).into_owned()),
            ),
        });
    }
}

/// The simulated device tree: the root, the children on its ports, and the
/// cables between them. It owns no socket and no terminal.
struct Tree {
    root: Sim,
    hub: Hub<Ask, MAX_PORTS>,
    children: Vec<Child>,
    rng: SmallRng,
    notes: Vec<String>,
}

impl Tree {
    fn new(cli: &SimulateCli, now: u64) -> io::Result<Self> {
        let role = match cli.children {
            0 => Role::Sensor,
            _ => Role::Hub,
        };
        let pulses = match role {
            Role::Hub => Pulses::own(!cli.no_gps, now),
            Role::Sensor => Pulses::own(!cli.no_pps, now),
        };
        let cable = Cable {
            pps_delay_ns: PPS_DELAY_NS,
            sync_latency_ns: cli.sync_latency * 1_000_000,
            sync_drop: cli.sync_drop,
            pps_present: !cli.no_pps,
            drift_ppm: cli.drift,
        };
        Ok(Self {
            root: Sim::new(cli, role, 0, pulses, now)?,
            hub: Hub::new(),
            children: (1..=cli.children)
                .map(|port| Child::new(cli, port, cable, now))
                .collect::<io::Result<Vec<_>>>()?,
            rng: seeded(),
            notes: Vec::new(),
        })
    }

    /// A host has connected: every device in the tree describes itself.
    fn connected(&mut self, now: u64, out: &mut impl Sink) {
        self.root.connected(now, out);
        self.each_child(now, out, |sim, now, wire| sim.connected(now, wire));
    }

    /// Put every child to work, and route what each one sends up.
    fn each_child(
        &mut self,
        now: u64,
        out: &mut impl Sink,
        act: impl Fn(&mut Sim, u64, &mut Wire),
    ) {
        for index in 0..self.children.len() {
            let mut wire = Wire::default();
            act(&mut self.children[index].sim, now, &mut wire);
            self.child_sent(index, wire, now, out);
        }
    }

    /// One packet from the host: the root answers what is addressed to it, and
    /// the hub routes the rest, editing it where it lies.
    fn handle(&mut self, packet: &mut [u8], now: u64, out: &mut impl Sink) {
        let Ok((view, _)) = PacketView::parse_prefix(packet) else {
            return;
        };
        if view.routing.is_empty() {
            return self.root.handle(view, now, out);
        }
        let mut down = Downlink::default();
        let mut seen = Seen::default();
        self.hub
            .handle(Input::FromHost(packet), now, out, &mut down, &mut seen);
        self.deliver(down, now, out);
        self.absorb(seen, now, out);
    }

    /// Everything due at `now`: the root's own work, its time on every cable,
    /// each child's work, and what the ports and calls have left.
    fn tick(&mut self, now: u64, out: &mut impl Sink) {
        self.root.tick(now, out);
        if let Some((at, announce)) = self.root.announced.take() {
            self.pulse_children(at);
            self.announce(&announce, at);
        }
        self.deliver_inflight(now, out);
        self.each_child(now, out, |sim, now, wire| sim.tick(now, wire));
        let mut down = Downlink::default();
        let mut seen = Seen::default();
        self.hub.handle(Input::Tick, now, out, &mut down, &mut seen);
        self.deliver(down, now, out);
        self.absorb(seen, now, out);
    }

    /// Power-cycle every device in the tree.
    fn reboot(&mut self, now: u64, out: &mut impl Sink) {
        self.root.reboot(now, out);
        self.each_child(now, out, |sim, now, wire| sim.reboot(now, wire));
    }

    /// Power-cycle the root alone, which the children then have to re-adopt.
    fn reboot_root(&mut self, now: u64, out: &mut impl Sink) {
        self.root.reboot(now, out);
    }

    /// Drop one sample from every device's clocks, as the keyboard asks.
    fn drop_now(&mut self) {
        self.root.drop_now();
        self.children
            .iter_mut()
            .for_each(|child| child.sim.drop_now());
    }

    /// Turn the simulated pulses on or off: every cable's, or the root's own
    /// when it stands alone.
    fn toggle_pps(&mut self) {
        let Some(first) = self.children.first() else {
            return self.root.toggle_pps();
        };
        let present = !first.cable.pps_present;
        for child in &mut self.children {
            child.cable.pps_present = present;
        }
        self.notes.push(match present {
            true => "PPS restored on every cable".to_string(),
            false => "PPS removed from every cable; the children fall into holdover".to_string(),
        });
    }

    /// Pull a child's cable out, or push it back in.
    fn toggle_plug(&mut self, port: u8) {
        let Some(index) = self.index_of(port) else {
            return;
        };
        let child = &mut self.children[index];
        child.plug = match child.plug {
            Plug::In => Plug::Out,
            Plug::Out => Plug::In,
        };
        child.inflight.clear();
        self.notes.push(match child.plug {
            Plug::In => format!("/{port}: cable plugged back in"),
            Plug::Out => format!("/{port}: cable pulled out"),
        });
    }

    /// Every note the tree has to print, each child's tagged with its route.
    fn drain_notes(&mut self) -> Vec<String> {
        let mut notes: Vec<String> = self
            .notes
            .drain(..)
            .chain(self.root.notes.drain(..))
            .collect();
        notes.extend(
            self.children
                .iter_mut()
                .enumerate()
                .flat_map(|(index, child)| {
                    let port = port_of(index);
                    child
                        .sim
                        .notes
                        .drain(..)
                        .map(move |note| format!("/{port}: {note}"))
                }),
        );
        notes
    }

    /// Deliver the root's pulse to every child, landing on whatever phase of
    /// its counter the cable and its own drift put it at.
    fn pulse_children(&mut self, at: u64) {
        for child in &mut self.children {
            if let (Plug::In, true) = (child.plug, child.cable.pps_present) {
                let edge = child.edge_at(at);
                child.sim.pulse(edge, at);
            }
        }
    }

    /// Put the root's time reference on each cable, as late and as lossy as
    /// the cable is.
    fn announce(&mut self, announce: &Announce, at: u64) {
        let mut down = Downlink::default();
        self.hub.announce(announce, &mut down);
        for (port, packet) in down.0 {
            let Some(index) = self.index_of(port) else {
                continue;
            };
            let Plug::In = self.children[index].plug else {
                continue;
            };
            let cable = self.children[index].cable;
            if self.rng.random_range(0..100) < cable.sync_drop {
                continue;
            }
            self.children[index]
                .inflight
                .push_back((at + cable.sync_latency_ns, packet));
        }
    }

    /// Hand each child every time reference the cable has got to it by now.
    fn deliver_inflight(&mut self, now: u64, out: &mut impl Sink) {
        for index in 0..self.children.len() {
            while let Some((at, packet)) = self.children[index].inflight.pop_front() {
                if at > now {
                    self.children[index].inflight.push_front((at, packet));
                    break;
                }
                self.hand(index, &packet, now, out);
            }
        }
    }

    /// Everything one child sent, on its way up through the hub.
    fn child_sent(&mut self, index: usize, mut wire: Wire, now: u64, out: &mut impl Sink) {
        let Plug::In = self.children[index].plug else {
            return;
        };
        let port = port_of(index);
        let mut down = Downlink::default();
        let mut seen = Seen::default();
        for packet in &mut wire.0 {
            self.hub.handle(
                Input::FromChild { port, packet },
                now,
                out,
                &mut down,
                &mut seen,
            );
        }
        self.deliver(down, now, out);
        self.absorb(seen, now, out);
    }

    /// Give each packet the hub routed to the child whose port it names.
    fn deliver(&mut self, down: Downlink, now: u64, out: &mut impl Sink) {
        for (port, packet) in down.0 {
            let Some(index) = self.index_of(port) else {
                continue;
            };
            let Plug::In = self.children[index].plug else {
                continue;
            };
            self.hand(index, &packet, now, out);
        }
    }

    /// Put one packet into a child, and route whatever it sends back up.
    fn hand(&mut self, index: usize, packet: &[u8], now: u64, out: &mut impl Sink) {
        let Ok((view, _)) = PacketView::parse_prefix(packet) else {
            return;
        };
        let mut wire = Wire::default();
        self.children[index].sim.handle(view, now, &mut wire);
        self.child_sent(index, wire, now, out);
    }

    /// Act on what the hub noticed: a child that appears is asked what it is.
    fn absorb(&mut self, seen: Seen, now: u64, out: &mut impl Sink) {
        for noticed in seen.0 {
            match noticed {
                Noticed::Plugged(port) => {
                    self.notes.push(format!("/{port}: plugged in"));
                    self.ask_name(port, now, out);
                }
                Noticed::Unplugged(port) => self.notes.push(format!("/{port}: unplugged")),
                Noticed::Named(port, Ok(name)) => self.notes.push(format!("/{port}: is a {name}")),
                Noticed::Named(port, Err(error)) => self
                    .notes
                    .push(format!("/{port}: did not answer dev.name ({error})")),
            }
        }
    }

    /// Ask a child that has just appeared what it is.
    fn ask_name(&mut self, port: u8, now: u64, out: &mut impl Sink) {
        let mut down = Downlink::default();
        let refused = match self
            .hub
            .call(port, "dev.name", &[], Ask::Name(port), now, &mut down)
        {
            Ok(_) => {
                self.deliver(down, now, out);
                return;
            }
            Err(CallError::Absent) => "nothing is plugged into it",
            Err(CallError::Full) => "no call slot is free",
            Err(CallError::TooLong) => "the request does not fit a packet",
        };
        self.notes
            .push(format!("/{port}: not asked its name, {refused}"));
    }

    /// The child on a port, if there is one.
    fn index_of(&self, port: u8) -> Option<usize> {
        usize::from(port)
            .checked_sub(1)
            .filter(|index| *index < self.children.len())
    }
}

/// The port a child sits on: the first is at `/1`, as a proxy's mounts are.
fn port_of(index: usize) -> u8 {
    index as u8 + 1
}

/// What runs the simulated tree: the socket it answers on, the client it
/// answers to, the clock it steps with, and the keyboard.
struct Runtime {
    socket: UdpSocket,
    client: Option<Client>,
    tree: Tree,
}

impl Runtime {
    fn new(cli: SimulateCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            client: None,
            tree: Tree::new(&cli, now_ns())?,
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
            self.step(|tree, now, sink| tree.tick(now, sink))?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// What the tree is, as the terminal sees it at startup.
    fn banner(&self, keyboard: bool) -> io::Result<()> {
        let port = self.socket.local_addr()?.port();
        let root = &self.tree.root;
        let settings = root.device.settings();
        terminal_println!("tio test listening on udp://0.0.0.0:{port}");
        match self.tree.children.first() {
            None => terminal_println!("  one device at /: {DEVICE_NAME} ({DEVICE_SERIAL})"),
            Some(first) => {
                terminal_println!(
                    "  a hub at /: {HUB_NAME} ({HUB_SERIAL}), {} second, announced once a second",
                    if root.pulses.pulsing() {
                        "a simulated GPS"
                    } else {
                        "no reference"
                    }
                );
                terminal_println!(
                    "  {} children at /1../{}, time references {} ms late, dropping {}%, drifting {} ppm",
                    self.tree.children.len(),
                    self.tree.children.len(),
                    first.cable.sync_latency_ns / 1_000_000,
                    first.cable.sync_drop,
                    first.cable.drift_ppm
                );
            }
        }
        terminal_println!(
            "  stream 1: 2 waveform channels, amplitude={} V frequency={} Hz noise={} V/sqrt(Hz) samplerate={} Hz segment={} s",
            settings.amplitude.get(),
            settings.frequency.get(),
            settings.noise.get(),
            root.sample_rate(),
            root.segment_seconds
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
        if root.no_drop {
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
                "  press d to drop one sample now, p to toggle the PPS, r to reboot everything, \
                 h to reboot the root, 1-9 to unplug a child, Ctrl-C to quit"
            );
        }
        terminal_println!(
            "  {} PPS, acquiring {} s after boot; dev.start, dev.stop, dev.restart",
            if root.pulses.pulsing() {
                "simulated"
            } else {
                "no"
            },
            AUTOSTART_SECONDS
        );
        terminal_println!(
            "  simulated flash: dev.conf.save keeps test.amplitude, test.frequency, \
             test.noise across a reboot, dev.conf.reset erases them; \
             dev.firmware.upload takes a signed package for COMM-USB rev 8 at its \
             cursor, dev.firmware.upgrade commits"
        );
        terminal_println!("  connect with: tio proxy udp4://127.0.0.1:{port}");
        Ok(())
    }

    /// Step the simulation, with the connected client as its sink.
    fn step(&mut self, act: impl FnOnce(&mut Tree, u64, &mut UdpSink<'_>)) -> io::Result<()> {
        let now = now_ns();
        let mut sink = UdpSink::new(&self.socket, self.client.map(|client| client.addr));
        act(&mut self.tree, now, &mut sink);
        self.tree
            .drain_notes()
            .into_iter()
            .for_each(|note| terminal_println!("{note}"));
        sink.finish()
    }

    fn handle_keyboard(&mut self) -> io::Result<bool> {
        while event::poll(Duration::from_millis(0))? {
            if let event::Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('d') => self.step(|tree, _, _| tree.drop_now())?,
                    KeyCode::Char('p') => self.step(|tree, _, _| tree.toggle_pps())?,
                    KeyCode::Char('r') => self.step(|tree, now, sink| tree.reboot(now, sink))?,
                    KeyCode::Char('h') => {
                        self.step(|tree, now, sink| tree.reboot_root(now, sink))?
                    }
                    KeyCode::Char(child @ '1'..='9') => {
                        self.step(move |tree, _, _| tree.toggle_plug(child as u8 - b'0'))?
                    }
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
                    match PacketView::parse_prefix(&buf[..size]).map(|(_, parsed)| parsed) {
                        Ok(parsed) if parsed == size => {
                            self.step(|tree, now, sink| tree.handle(&mut buf[..size], now, sink))?;
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
                self.step(|tree, now, sink| tree.connected(now, sink))?;
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

/// A device's synchronizer: its own wall clock to start from, a simulated
/// pulse to follow, and no oscillator to steer.
fn synchronizer(role: Role, session: SessionId, serial: &str, now: u64) -> Synchronizer {
    Synchronizer::new(
        CounterDomain::new(PPS_PERIOD),
        PulseConfig::with_edge_tolerance(ticks(PPS_TOLERANCE_NS, PPS_PERIOD)),
        Reference {
            identity: ReferenceIdentity::new(sync::Epoch::UNIX, session, serial.as_bytes()),
            second: (now / NANOS_PER_SECOND) as u32,
        },
        None,
        autostart_of(role),
    )
}

/// What a device of this role calls itself. Its serial stands in for the
/// unique id a real board reads out of its MCU.
fn identity(role: Role, serial: &str) -> io::Result<Identity> {
    let name = match role {
        Role::Hub => HUB_NAME,
        Role::Sensor => DEVICE_NAME,
    };
    Identity::new(name, desc_of(role, false), serial, DEVICE_FIRMWARE)
        .map(|identity| identity.hardware(name, DEVICE_MCU, serial.to_owned().leak().as_bytes(), 1))
        .ok_or_else(|| invalid_input("identity too long"))
}

/// What `dev.desc` says, before an uploaded image has taken and after.
fn desc_of(role: Role, upgraded: bool) -> &'static str {
    match (role, upgraded) {
        (Role::Hub, false) | (Role::Hub, true) => HUB_DESC,
        (Role::Sensor, false) => DEVICE_DESC,
        (Role::Sensor, true) => UPGRADED_DESC,
    }
}

/// The serial of the device at `port`, with 0 the root.
fn serial_of(role: Role, port: u8) -> String {
    match role {
        Role::Hub => HUB_SERIAL.to_string(),
        Role::Sensor if port == 0 => DEVICE_SERIAL.to_string(),
        Role::Sensor => format!("SIM{port:04}"),
    }
}

/// Seconds from boot to an automatic start. A hub has nothing to acquire.
fn autostart_of(role: Role) -> u8 {
    match role {
        Role::Hub => 0,
        Role::Sensor => AUTOSTART_SECONDS,
    }
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

/// The streams a boot starts: none on a hub, and on a sensor no decimation, no
/// anti-alias filter, and a fresh segment ring on each.
fn boot_streams(role: Role, rate: NonZeroU32) -> io::Result<Vec<Stream<SEGMENTS>>> {
    let stream = |id: u8, def: &'static StreamDef, rate: NonZeroU32| {
        let params = Params {
            rate,
            decimation: NonZeroU32::MIN,
            enabled: true,
        };
        Stream::new(
            StreamId::new(id),
            def,
            Box::leak(Box::new(filter::None)),
            params,
        )
        .ok_or_else(|| invalid_input("stream sample is too large for a TIO packet"))
    };
    match role {
        Role::Hub => Ok(Vec::new()),
        Role::Sensor => Ok(vec![
            stream(1, &SINE_DEF, rate)?,
            stream(2, &STATUS_DEF, rate)?,
            stream(3, &AUX_DEF, AUX_SAMPLE_RATE)?,
        ]),
    }
}

/// The sample clocks a boot starts, which a hub has none of.
fn boot_clocks(role: Role, rate: NonZeroU32, seconds: u32) -> io::Result<Vec<Clock>> {
    match role {
        Role::Hub => Ok(Vec::new()),
        Role::Sensor => Ok(vec![
            Clock::new(rate, seconds, &[Signal::Sine, Signal::Status])?,
            Clock::new(AUX_SAMPLE_RATE, seconds, &[Signal::Aux])?,
        ]),
    }
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

fn next_log_delay(rng: &mut SmallRng) -> u64 {
    LOG_MESSAGE_MIN_INTERVAL_NS + (LOG_MESSAGE_JITTER_NS as f64 * rng.random::<f64>()) as u64
}

fn next_capture_sample_count(rng: &mut SmallRng) -> usize {
    rng.random_range(CAPTURE_SAMPLE_COUNT_MIN..=CAPTURE_SAMPLE_COUNT_MAX)
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
    use twinleaf_device::data::metadata::{self, Streams};
    use twinleaf_device::rpc::REPLY_MAX;
    use twinleaf_device::storage::update::{FORMAT_VERSION, HEADER_SIZE};
    use twinleaf_device::sync::ReferenceState;

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
    fn sine(sim: &Sim) -> &twinleaf_device::data::Segment {
        sim.streams[Signal::Sine as usize].current()
    }

    /// What the sine stream's current segment says about itself on the wire.
    fn sine_flags(sim: &Sim) -> data::SegmentFlags {
        sim.streams[Signal::Sine as usize]
            .segment(CURRENT_SEGMENT)
            .expect("a current segment")
            .flags
    }

    /// A lone device at the instant it booted.
    fn sim(args: &[&str]) -> Sim {
        let cli = cli(args);
        let pulses = Pulses::own(!cli.no_pps, BOOT);
        Sim::new(&cli, Role::Sensor, 0, pulses, BOOT).unwrap()
    }

    fn cli(args: &[&str]) -> SimulateCli {
        SimulateCli::parse_from([&["tio-simulate", "--port", "0"], args].concat())
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

    /// What the simulation answered an RPC with: a reply, or the error it
    /// refused the call with.
    fn answer(
        sim: &mut Sim,
        name: &[u8],
        args: &[u8],
        sent: &mut Sent,
    ) -> Result<Vec<u8>, RpcError> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let method = twinleaf::proto::rpc::Method::ByName(name);
        let len = twinleaf::proto::rpc::write_request(&mut buf, RpcRequestId::new(1), method, args)
            .unwrap();
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        sim.handle(view, BOOT, sent);
        let view = *sent.views().last().expect("a reply");
        match Answer::parse(view.header.ptype, view.payload) {
            Some(Answer::Reply(reply)) => Ok(reply.value.to_vec()),
            Some(Answer::Error(error)) => Err(error.error()),
            None => panic!("an answer"),
        }
    }

    /// The reply the simulation answered an RPC with.
    fn call(sim: &mut Sim, name: &[u8], args: &[u8], sent: &mut Sent) -> Vec<u8> {
        answer(sim, name, args, sent).unwrap_or_else(|error| panic!("{error:?}"))
    }

    /// What `tio upgrade` sends in one `dev.firmware.upload`, and how big an
    /// image the tests package: two whole chunks of it.
    const CHUNK: usize = 288;
    const IMAGE_SIZE: usize = 2 * CHUNK;

    /// The image a package carries.
    fn image() -> Vec<u8> {
        (0..IMAGE_SIZE).map(|byte| byte as u8).collect()
    }

    /// A signed package for the board the simulator answers as, as
    /// `firmware-pack` writes one: the header, then the image.
    fn package() -> Vec<u8> {
        let mut header = vec![0u8; HEADER_SIZE];
        header[..8].copy_from_slice(PACKAGE.magic.as_bytes());
        header[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[10..12].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
        header[12..20].copy_from_slice(PACKAGE.board_id.as_bytes());
        header[20..22].copy_from_slice(&PACKAGE.hw_rev.to_le_bytes());
        header[22..24].copy_from_slice(&1u16.to_le_bytes());
        header[24..28].copy_from_slice(&(IMAGE_SIZE as u32).to_le_bytes());
        [header, image()].concat()
    }

    /// The SETTING announcements the simulation sent, in order.
    fn announcements(sent: &Sent) -> Vec<(Vec<u8>, Vec<u8>)> {
        sent.views()
            .iter()
            .filter(|view| view.header.ptype == PacketType::SETTING)
            .map(|view| Announcement::parse(view.payload).unwrap())
            .map(|setting| (setting.name.to_vec(), setting.reply.to_vec()))
            .collect()
    }

    /// A package for another board is refused at its header, before any of it
    /// is written.
    #[test]
    fn a_package_the_board_is_not_for_is_refused_at_its_header() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        let cursor = b"dev.firmware.upload";
        let mut package = package();
        package[..8].copy_from_slice(b"ETHANFW\0");

        assert_eq!(
            answer(&mut sim, cursor, &package[..HEADER_SIZE], &mut sent),
            Err(RpcError::Invalid)
        );
        assert_eq!(call(&mut sim, cursor, &[], &mut sent), 0u32.to_le_bytes());
        assert!(sim.flash.image.is_empty());
    }

    /// The header counts as no image bytes, and every chunk after it moves the
    /// cursor by its own length — which is what a resuming host reads back.
    #[test]
    fn the_cursor_counts_the_image_bytes_the_chunks_carried() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        let cursor = b"dev.firmware.upload";
        let package = package();

        assert_eq!(call(&mut sim, cursor, &[], &mut sent), 0u32.to_le_bytes());
        for (sent_chunks, chunk) in package.chunks(CHUNK).enumerate() {
            assert_eq!(
                call(&mut sim, cursor, chunk, &mut sent),
                ((sent_chunks * CHUNK) as u32).to_le_bytes(),
                "after chunk {sent_chunks}"
            );
        }
        assert_eq!(
            call(&mut sim, cursor, &[], &mut sent),
            (IMAGE_SIZE as u32).to_le_bytes()
        );
        assert_eq!(sim.flash.image, image());
    }

    /// `dev.firmware.abort` throws the part-taken package away, so the next
    /// host's header is read as a header rather than as image data.
    #[test]
    fn abort_puts_the_cursor_back_for_the_next_host() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        let cursor = b"dev.firmware.upload";
        let package = package();

        call(&mut sim, cursor, &package[..HEADER_SIZE], &mut sent);
        call(
            &mut sim,
            cursor,
            &package[HEADER_SIZE..HEADER_SIZE + CHUNK],
            &mut sent,
        );
        assert_eq!(
            call(&mut sim, cursor, &package[..HEADER_SIZE], &mut sent),
            (IMAGE_SIZE as u32).to_le_bytes(),
            "a header resent mid-upload is image data, which is why a host \
             that cannot place its cursor aborts before starting over"
        );

        call(&mut sim, b"dev.firmware.abort", &[], &mut sent);
        assert_eq!(call(&mut sim, cursor, &[], &mut sent), 0u32.to_le_bytes());
        for chunk in package.chunks(CHUNK) {
            call(&mut sim, cursor, chunk, &mut sent);
        }
        assert_eq!(sim.flash.image, image(), "the next header cleared the slot");
    }

    #[test]
    fn an_upgrade_needs_the_whole_image_and_the_reboot_comes_up_on_it() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        let cursor = b"dev.firmware.upload";
        let package = package();

        assert_eq!(
            answer(&mut sim, b"dev.firmware.upgrade", &[], &mut sent),
            Err(RpcError::State)
        );
        for chunk in package[..package.len() - CHUNK].chunks(CHUNK) {
            call(&mut sim, cursor, chunk, &mut sent);
        }
        assert_eq!(
            answer(&mut sim, b"dev.firmware.upgrade", &[], &mut sent),
            Err(RpcError::State),
            "the last chunk is still missing"
        );

        call(
            &mut sim,
            cursor,
            &package[package.len() - CHUNK..],
            &mut sent,
        );
        call(&mut sim, b"dev.firmware.upgrade", &[], &mut sent);
        assert_eq!(
            call(&mut sim, b"dev.desc", &[], &mut sent),
            UPGRADED_DESC.as_bytes()
        );
        assert_eq!(
            answer(&mut sim, b"dev.firmware.abort", &[], &mut sent),
            Err(RpcError::State),
            "an armed swap is not taken back"
        );

        sim.reboot(BOOT, &mut sent);
        assert_eq!(
            call(&mut sim, b"dev.desc", &[], &mut sent),
            UPGRADED_DESC.as_bytes()
        );
        assert_eq!(call(&mut sim, cursor, &[], &mut sent), 0u32.to_le_bytes());
    }

    #[test]
    fn a_saved_setting_comes_back_after_a_reboot_and_an_unsaved_one_does_not() {
        let mut sim = sim(&["--amplitude", "1"]);
        let mut sent = Sent::default();
        let amplitude = 7.5f64.to_le_bytes();
        call(&mut sim, b"test.amplitude", &amplitude, &mut sent);
        call(&mut sim, b"test.enable", &[0], &mut sent);
        call(&mut sim, b"dev.conf.save", &[], &mut sent);

        let mut sent = Sent::default();
        sim.reboot(BOOT, &mut sent);
        assert_eq!(sim.device.settings().amplitude.get(), 7.5);
        assert!(sim.device.settings().enable.get());
        assert_eq!(
            announcements(&sent),
            [
                (
                    b"rpc.hash".to_vec(),
                    sim.device.hash().to_le_bytes().to_vec()
                ),
                (b"test.amplitude".to_vec(), amplitude.to_vec()),
            ]
        );

        let mut sent = Sent::default();
        assert_eq!(
            call(&mut sim, b"settings.version", &[], &mut sent),
            1u32.to_le_bytes()
        );

        call(&mut sim, b"dev.conf.reset", &[], &mut sent);
        assert_eq!(sim.device.settings().amplitude.get(), 1.0);
    }

    /// The walk that answers a name is the walk the table is declared from,
    /// so every entry the group carries is the entry the table lists, in the
    /// order its ids follow the standard entries'.
    #[test]
    fn the_table_describes_the_settings_it_answers() {
        let mut settings = Settings::new(&cli(&[]), AUTOSTART_SECONDS);
        let mut declared = Vec::new();
        settings.entries(&mut |entry| declared.push(entry.spec()));
        assert_eq!(
            &RPCS[STANDARD.len()..STANDARD.len() + declared.len()],
            declared
        );

        let answered: Vec<&str> = RPCS[STANDARD.len() + declared.len()..]
            .iter()
            .map(|spec| spec.name)
            .collect();
        assert_eq!(
            answered,
            ["test.go", "test.capture"],
            "what is left is what the simulator answers itself"
        );
    }

    /// What the control machine answers for every platform, which the
    /// simulator used to refuse.
    #[test]
    fn the_standard_rpcs_the_machine_answers_reach_the_host() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        assert_eq!(call(&mut sim, b"dev.systime", &[], &mut sent), [0; 8]);
        assert_eq!(call(&mut sim, b"dev.uptime", &[], &mut sent), [0; 4]);
        assert_eq!(
            call(&mut sim, b"rpc.match", b"test.ampl", &mut sent),
            b"test.amplitude"
        );
        // The hardware half of the identity, which the simulator makes up.
        assert_eq!(
            call(&mut sim, b"dev.uid", &[], &mut sent),
            DEVICE_SERIAL.as_bytes()
        );
        assert_eq!(
            call(&mut sim, b"dev.model", &[], &mut sent),
            DEVICE_NAME.as_bytes()
        );
        assert_eq!(call(&mut sim, b"dev.revision", &[], &mut sent), [1, 0]);
        assert_eq!(
            answer(&mut sim, b"dev.mcu.model", &[], &mut sent),
            Err(RpcError::NotFound)
        );
        call(&mut sim, b"dev.priv", b"895895", &mut sent);
        assert_eq!(
            call(&mut sim, b"dev.mcu.model", &[], &mut sent),
            DEVICE_MCU.as_bytes()
        );
    }

    /// `dev.reboot` is answered before the device goes away, and what comes
    /// back is a new session.
    #[test]
    fn dev_reboot_replies_and_comes_back_on_a_new_session() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();
        let session = sim.device.session;
        let mut buf = [0u8; Packet::MAX_SIZE];
        let method = twinleaf::proto::rpc::Method::ByName(b"dev.reboot");
        let len = twinleaf::proto::rpc::write_request(&mut buf, RpcRequestId::new(1), method, &[])
            .unwrap();
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        sim.handle(view, BOOT, &mut sent);

        // A host is told why the device is about to go away, answered, and
        // then met by the device coming back up.
        let views = sent.views();
        assert_eq!(views[0].header.ptype, PacketType::LOG);
        assert!(
            matches!(Answer::parse(views[1].header.ptype, views[1].payload), Some(Answer::Reply(reply)) if reply.value.is_empty())
        );
        assert_eq!(views[2].header.ptype, PacketType::SETTING, "rpc.hash");
        assert_ne!(sim.device.session, session);
        assert!(!sim.acquiring);
    }

    #[test]
    fn a_setting_write_is_announced_and_counted() {
        let mut sim = sim(&[]);
        let mut sent = Sent::default();

        let value = 2.5f64.to_le_bytes();
        assert_eq!(call(&mut sim, b"test.amplitude", &value, &mut sent), value);
        assert_eq!(sim.device.settings().amplitude.get(), 2.5);

        assert_eq!(
            announcements(&sent),
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
        assert_eq!(sim.device.settings().amplitude.get(), 1.0);
        assert!(sim.device.settings().enable.get());

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
        assert_eq!(capture.status(), Status::Capturing);

        capture.update(499);
        assert!(capture.locked());

        capture.update(500);
        assert!(!capture.locked());
        assert_eq!(capture.status(), Status::Done);
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
        assert_eq!(segment.filter_cutoff, 0.0);
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
        assert!(!sim.holdover);
    }

    /// The 'p' key's demonstration: the pulses go away, the segment ends, and
    /// the one that opens carries the holdover flag.
    #[test]
    fn losing_the_pulses_rolls_over_and_flags_the_segment() {
        let (mut sim, now) = acquiring(&["--samplerate", "4"]);
        let mut sent = Sent::default();
        let mut at = now + NANOS_PER_SECOND;
        sim.tick(at, &mut sent);
        assert!(!sim.holdover);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE
        );

        sim.toggle_pps();
        for _ in 0..4 {
            at += NANOS_PER_SECOND;
            sim.tick(at, &mut sent);
        }
        assert!(sim.holdover);
        assert_eq!(sim.sync.status().announced, TimeStatus::Holdover);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE | data::SegmentFlags::HOLDOVER
        );
        assert_eq!(sine(&sim).id().value(), 1);
    }

    /// D8: a device whose pulses never arrived has nothing to hold over from,
    /// so the segments it opens carry no flag.
    #[test]
    fn a_device_that_never_had_pulses_opens_unflagged_segments() {
        let (mut sim, now) = acquiring(&["--samplerate", "4", "--no-pps"]);
        sim.tick(now + NANOS_PER_SECOND, &mut Sent::default());
        assert!(!sim.holdover);
        assert_eq!(sim.sync.status().announced, TimeStatus::FreeRun);
        assert_eq!(
            sine_flags(&sim),
            data::SegmentFlags::VALID | data::SegmentFlags::ACTIVE
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
        let mut rng = SmallRng::seed_from_u64(1);
        let mut counts = Vec::new();
        for _ in 0..8 {
            counts.push(next_capture_sample_count(&mut rng));
        }

        assert!(counts
            .iter()
            .all(|count| (CAPTURE_SAMPLE_COUNT_MIN..=CAPTURE_SAMPLE_COUNT_MAX).contains(count)));
        assert!(counts.windows(2).any(|pair| pair[0] != pair[1]));
    }

    /// A hub with `children` sensors under it, at the instant it booted.
    fn tree(args: &[&str]) -> Tree {
        let cli = cli(&[&["--children", "2"], args].concat());
        Tree::new(&cli, BOOT).unwrap()
    }

    /// Step the whole tree for `seconds`, a hundred times a second, and
    /// return the instant it reached.
    fn run(tree: &mut Tree, from: u64, seconds: u64) -> u64 {
        let mut sent = Sent::default();
        let mut at = from;
        for _ in 0..seconds * 100 {
            at += NANOS_PER_SECOND / 100;
            tree.tick(at, &mut sent);
        }
        at
    }

    /// What the tree replied to an RPC addressed along `hops`, if anything.
    fn ask(tree: &mut Tree, hops: &[u8], name: &[u8], now: u64) -> Option<Vec<u8>> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let method = twinleaf::proto::rpc::Method::ByName(name);
        let mut len =
            twinleaf::proto::rpc::write_request(&mut buf, RpcRequestId::new(5), method, &[])
                .unwrap();
        for &hop in hops {
            len = twinleaf::proto::route::push_hop(&mut buf, hop).unwrap();
        }
        let mut sent = Sent::default();
        tree.handle(&mut buf[..len], now, &mut sent);
        sent.views().iter().find_map(
            |view| match Answer::parse(view.header.ptype, view.payload) {
                Some(Answer::Reply(reply)) if reply.req_id == RpcRequestId::new(5) => {
                    Some(reply.value.to_vec())
                }
                Some(Answer::Reply(_)) | Some(Answer::Error(_)) | None => None,
            },
        )
    }

    /// The tree answers for itself at the root and for each child at its own
    /// route, under the id the host chose.
    #[test]
    fn a_route_reaches_the_child_that_answers_it() {
        let mut tree = tree(&[]);
        let at = run(&mut tree, BOOT, 1);

        assert_eq!(
            ask(&mut tree, &[], b"dev.name", at),
            Some(b"tio-hub".to_vec())
        );
        assert_eq!(
            ask(&mut tree, &[1], b"dev.name", at),
            Some(b"tio-test".to_vec())
        );
        assert_eq!(
            ask(&mut tree, &[2], b"dev.session", at),
            Some(tree.children[1].sim.device.session.to_le_bytes().to_vec())
        );
        assert_eq!(ask(&mut tree, &[3], b"dev.name", at), None);
    }

    /// The children follow the hub's second: they adopt its timeline, and the
    /// segments they open carry its serial and session.
    #[test]
    fn the_children_adopt_the_hubs_timeline_and_name_it_in_their_segments() {
        let mut tree = tree(&["--samplerate", "4"]);
        let at = run(&mut tree, BOOT, 6);
        for child in &tree.children {
            assert_eq!(child.sim.sync.status().reference, ReferenceState::Upstream);
        }

        run(&mut tree, at, 6);
        let session = tree.root.device.session;
        for child in &tree.children {
            assert!(child.sim.acquiring);
            assert_eq!(sine(&child.sim).timeref().serial, HUB_SERIAL);
            assert_eq!(sine(&child.sim).timeref().session, session);
            assert!(!child.sim.holdover);
        }
    }

    /// A hub reboot is a new timebase: the children flag the segment they are
    /// in as holdover, then adopt the session the hub came back with.
    #[test]
    fn a_hub_reboot_flags_holdover_and_the_children_re_adopt() {
        let mut tree = tree(&["--samplerate", "4"]);
        let at = run(&mut tree, BOOT, 10);
        let rebooted = tree.root.device.session;

        let mut sent = Sent::default();
        tree.reboot_root(at, &mut sent);
        let at = run(&mut tree, at, 3);
        assert_ne!(tree.root.device.session, rebooted);
        assert!(tree.children.iter().all(|child| {
            child.sim.sync.status().announced == TimeStatus::Holdover
                && sine_flags(&child.sim).contains(data::SegmentFlags::HOLDOVER)
        }));

        run(&mut tree, at, 12);
        let session = tree.root.device.session;
        for child in &tree.children {
            assert_eq!(sine(&child.sim).timeref().session, session);
            assert_eq!(sine(&child.sim).timeref().serial, HUB_SERIAL);
        }
    }

    /// A tree whose time references arrive `latency_ms` late, run until its children have
    /// made up their minds, and the second its hub is on at that instant.
    fn cabled(latency_ms: &str) -> (Tree, u32) {
        let mut tree = tree(&["--samplerate", "4", "--sync-latency", latency_ms]);
        run(&mut tree, BOOT, 14);
        let hub_second = tree.root.sync.status().active.second;
        (tree, hub_second)
    }

    /// R11: the arrival window catches a cable most of a second long, and
    /// nothing catches one past it: the child adopts a timeline a second off.
    #[test]
    fn a_cable_past_one_second_adopts_a_timeline_one_second_off() {
        let (tree, hub_second) = cabled("100");
        for child in &tree.children {
            let status = child.sim.sync.status();
            assert_eq!(status.reference, ReferenceState::Upstream);
            assert_eq!(status.late_references, 0);
            assert_eq!(status.active.second, hub_second);
            assert_eq!(sine(&child.sim).timeref().serial, HUB_SERIAL);
        }

        let (tree, _) = cabled("950");
        for (index, child) in tree.children.iter().enumerate() {
            let status = child.sim.sync.status();
            assert!(status.late_references > 0);
            assert_eq!(status.reference, ReferenceState::Local);
            assert_eq!(
                sine(&child.sim).timeref().serial.as_str(),
                format!("SIM{:04}", index + 1)
            );
        }

        let (tree, hub_second) = cabled("1200");
        for child in &tree.children {
            let status = child.sim.sync.status();
            assert_eq!(status.reference, ReferenceState::Upstream);
            assert_eq!(status.late_references, 0);
            assert_eq!(status.active.second, hub_second - 1);
            assert_eq!(sine(&child.sim).timeref().serial, HUB_SERIAL);
        }
    }

    /// An unplugged child is gone: nothing answers its route, and two missed
    /// heartbeats later the hub says so.
    #[test]
    fn an_unplugged_child_answers_nothing_and_is_reported_gone() {
        let mut tree = tree(&["--samplerate", "4"]);
        let at = run(&mut tree, BOOT, 2);
        tree.drain_notes();

        tree.toggle_plug(1);
        assert_eq!(ask(&mut tree, &[1], b"dev.name", at), None);

        let at = run(&mut tree, at, 1);
        assert!(tree
            .drain_notes()
            .iter()
            .any(|note| note == "/1: unplugged"));
        assert_eq!(ask(&mut tree, &[1], b"dev.name", at), None);
        assert_eq!(
            ask(&mut tree, &[2], b"dev.name", at),
            Some(b"tio-test".to_vec())
        );

        tree.toggle_plug(1);
        let at = run(&mut tree, at, 1);
        assert_eq!(
            ask(&mut tree, &[1], b"dev.name", at),
            Some(b"tio-test".to_vec())
        );
    }
}

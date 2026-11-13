//! One simulated device. The `twinleaf-device` machine answers its packets
//! and streams its samples, and the board around it supplies the rest. That
//! is the waveforms, the capture buffers, the stored configuration and the
//! firmware slot, all in memory.
//!
//! It serves three captures. `test.capture` is the sine as it streams.
//! `fid.capture` is one shot of a free induction decay magnetometer as its
//! ADC reads it out, and `phase.capture` is the unwrapped phase of such a
//! shot. The precession frequency both read wanders slowly about
//! `fid.frequency`, so each shot differs from the last, and two shots taken
//! at the same instant precess alike.

use std::num::NonZeroU32;

use twinleaf::proto::capture::{CaptureMetadata, METADATA_VERSION};
use twinleaf::proto::data::DataType;
use twinleaf::proto::log::LogLevel;
use twinleaf::proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf::proto::{BoardId, DeviceRoute, FirmwareMagic, HwRev, SessionId, StreamId};
use twinleaf_device::data::{filter, ColumnDef, DataStream, Params, StreamDef};
use twinleaf_device::device::{Action, Announcer, Entry, Group, Identity};
use twinleaf_device::rpc::{Access, Capture, Reply, RpcSpec, Selector, Setting, Status, STANDARD};
use twinleaf_device::sim::{self, Port, Sim, Slot};
use twinleaf_device::storage::update::{Package, Upload};
use twinleaf_device::Sink;

use super::{unix_duration, GaussianRng};
use crate::{SimulateCli, UploadKind};

const DEVICE_NAME: &str = "tio-test";
const DEVICE_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000001-DEV]";
/// The description a firmware upgrade leaves, so a re-read shows a new build.
const UPGRADED_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000002]";
pub(super) const DEVICE_SERIAL: &str = "SIM0001";
const DEVICE_FIRMWARE: &str = "twinleaf-rust-test";
/// What `dev.priv` takes.
const PASSWORD: &str = "tio-test";

/// Segments each stream keeps, the most a ring holds.
const SEGMENTS: usize = 7;
pub(super) const AUX_SAMPLE_RATE: u32 = 25;
pub(super) const AUX_WAVE_FREQUENCY: f64 = 0.25;
pub(super) const SIGNAL_LEVEL: u8 = 234;
const SAMPLE_DROP_INTERVAL_SECONDS: f64 = 60.0;
const SAMPLE_DROP_JITTER_SECONDS: f64 = 30.0;
const LOG_MESSAGE_MIN_INTERVAL_NS: u64 = 1_500_000_000;
const LOG_MESSAGE_JITTER_NS: u64 = 4_000_000_000;
pub(super) const CAPTURE_DELAY_NS: u64 = 500_000_000;
const CAPTURE_BLOCK_SIZE: u16 = 256;
pub(super) const CAPTURE_SAMPLES_MIN: usize = 800;
pub(super) const CAPTURE_SAMPLES_MAX: usize = 1200;
/// Samples a second the ADC behind `fid.capture` takes.
pub(super) const FID_SAMPLE_RATE: f64 = 2_000_000.0;
/// Shots a second the magnetometer takes.
pub(super) const FID_REPETITION_RATE: f64 = 1_000.0;
/// The part of each repetition the ADC measures. The rest pumps.
pub(super) const FID_MEASURED: f64 = 0.75;
/// Samples of one shot, 750 us at 2 MS/s.
pub(super) const FID_SAMPLES: usize =
    (FID_SAMPLE_RATE / FID_REPETITION_RATE * FID_MEASURED) as usize;
/// Volts one count of the ADC is, 16 bits across 2.5 V either side of zero.
pub(super) const FID_VOLTS_PER_COUNT: f64 = 2.5 / 32768.0;
/// How long after its trigger a shot is ready, one repetition. A device
/// waits for its next shot to begin and then measures it, which takes no
/// longer.
pub(super) const FID_DELAY_NS: u64 = (1e9 / FID_REPETITION_RATE) as u64;
/// How far the precession frequency wanders from `fid.frequency`, as a
/// part of it, one standard deviation.
const FID_WANDER: f64 = 1e-3;
/// How long the wander takes to forget where it was, in seconds.
const FID_WANDER_SECONDS: f64 = 1.0;
/// The bytes of an image's upload chunk that are envelope rather than image.
const CHUNK_ENVELOPE: usize = 32;
/// Packets each lane of the host's port holds, more than one tick sends.
const PORT_CAPACITY: usize = 1 << 16;

/// What a signed package for the simulator names.
pub(super) const PACKAGE: Package = Package {
    magic: FirmwareMagic::new(*b"TIOTEST\0"),
    board_id: BoardId::from_ascii("tio-test"),
    hw_rev: HwRev::new(1),
    development: true,
    image_max: 1 << 20,
};

static SINE: StreamDef = StreamDef {
    name: "sine",
    columns: &[
        ColumnDef {
            name: "sine",
            units: "V",
            data_type: DataType::F64,
            description: "Noisy sine wave",
        },
        ColumnDef {
            name: "cosine",
            units: "V",
            data_type: DataType::F64,
            description: "Noisy quadrature wave",
        },
    ],
};

static STATUS: StreamDef = StreamDef {
    name: "status",
    columns: &[
        ColumnDef {
            name: "status",
            units: "",
            data_type: DataType::U8,
            description: "Mirrors the test.status RPC",
        },
        ColumnDef {
            name: "signal_level",
            units: "",
            data_type: DataType::U8,
            description: "Fixed simulated signal level",
        },
    ],
};

static AUX: StreamDef = StreamDef {
    name: "aux",
    columns: &[
        ColumnDef {
            name: "triangle",
            units: "arb",
            data_type: DataType::F64,
            description: "Triangle wave",
        },
        ColumnDef {
            name: "sawtooth",
            units: "arb",
            data_type: DataType::F64,
            description: "Sawtooth wave",
        },
    ],
};

/// The board's own entries after the standard table.
pub(super) struct Board {
    amplitude: Setting<f64>,
    frequency: Setting<f64>,
    noise: Setting<f64>,
    status: Setting<u8>,
    enable: Setting<bool>,
    go: Go,
    fid_frequency: Setting<f64>,
    fid_t2: Setting<f64>,
    fid_amplitude: Setting<f64>,
    fid_noise: Setting<f64>,
    samplerate: u32,
}

/// Refuses a value that is negative or not finite.
fn nonnegative(value: f64) -> Result<f64, RpcError> {
    match value.is_finite() && value >= 0.0 {
        true => Ok(value),
        false => Err(RpcError::Range),
    }
}

/// Refuses a value that is not finite or not above zero.
fn positive(value: f64) -> Result<f64, RpcError> {
    match value.is_finite() && value > 0.0 {
        true => Ok(value),
        false => Err(RpcError::Range),
    }
}

/// One shot's decay as the board's entries set it.
#[derive(Clone, Copy)]
struct Decay {
    /// Volts at the start of the shot.
    amplitude: f64,
    /// Seconds the envelope takes to fall to 1/e.
    t2: f64,
    /// Volts of noise in each sample, one standard deviation.
    noise: f64,
}

impl Decay {
    /// The envelope `t` seconds into the shot, in volts.
    fn envelope(&self, t: f64) -> f64 {
        self.amplitude * (-t / self.t2).exp()
    }
}

impl Board {
    fn new(cli: &SimulateCli) -> Self {
        Self {
            amplitude: Setting::new("test.amplitude", cli.amplitude).checked(nonnegative),
            frequency: Setting::new("test.frequency", cli.frequency).checked(nonnegative),
            noise: Setting::new("test.noise", cli.noise).checked(nonnegative),
            status: Setting::new("test.status", 0),
            enable: Setting::new("test.enable", true),
            go: Go,
            fid_frequency: Setting::new("fid.frequency", cli.fid_frequency).checked(nonnegative),
            fid_t2: Setting::new("fid.t2", cli.fid_t2).checked(positive),
            fid_amplitude: Setting::new("fid.amplitude", cli.fid_amplitude).checked(nonnegative),
            fid_noise: Setting::new("fid.noise", cli.fid_noise).checked(nonnegative),
            samplerate: cli.samplerate,
        }
    }

    /// The precession frequency the shots wander about, in Hz.
    pub(super) fn fid_frequency(&self) -> f64 {
        self.fid_frequency.get()
    }

    /// The decay time of a shot's envelope, in seconds.
    pub(super) fn fid_t2(&self) -> f64 {
        self.fid_t2.get()
    }

    /// A shot's amplitude as it starts, in volts.
    pub(super) fn fid_amplitude(&self) -> f64 {
        self.fid_amplitude.get()
    }

    /// The noise in each sample of a shot, in volts.
    pub(super) fn fid_noise(&self) -> f64 {
        self.fid_noise.get()
    }

    fn decay(&self) -> Decay {
        Decay {
            amplitude: self.fid_amplitude(),
            t2: self.fid_t2(),
            noise: self.fid_noise(),
        }
    }

    pub(super) fn amplitude(&self) -> f64 {
        self.amplitude.get()
    }

    pub(super) fn frequency(&self) -> f64 {
        self.frequency.get()
    }

    pub(super) fn noise(&self) -> f64 {
        self.noise.get()
    }

    pub(super) fn status(&self) -> u8 {
        self.status.get()
    }
}

impl Group for Board {
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
        visit(Entry::Setting(&mut self.amplitude));
        visit(Entry::Setting(&mut self.frequency));
        visit(Entry::Setting(&mut self.noise));
        visit(Entry::Setting(&mut self.status));
        visit(Entry::Setting(&mut self.enable));
        visit(Entry::Action(&mut self.go));
        visit(Entry::Setting(&mut self.fid_frequency));
        visit(Entry::Setting(&mut self.fid_t2));
        visit(Entry::Setting(&mut self.fid_amplitude));
        visit(Entry::Setting(&mut self.fid_noise));
    }
}

impl sim::Board<SEGMENTS> for Board {
    fn streams(&self) -> Vec<DataStream<SEGMENTS>> {
        let stream = |id: u8, def: &'static StreamDef, rate: u32| {
            let params = Params {
                rate: NonZeroU32::new(rate).expect("a sample rate is never zero"),
                decimation: NonZeroU32::MIN,
                enabled: true,
            };
            let filter = Box::leak(Box::new(filter::None));
            DataStream::new(StreamId::new(id), def, filter, params).expect("a sample fits a packet")
        };
        vec![
            stream(1, &SINE, self.samplerate),
            stream(2, &STATUS, self.samplerate),
            stream(3, &AUX, AUX_SAMPLE_RATE),
        ]
    }
}

/// `test.go`, which only says it ran.
struct Go;

impl Action for Go {
    fn spec(&self) -> RpcSpec {
        RpcSpec::action("test.go")
    }

    fn run(&mut self, _to: &mut Announcer<'_>) -> Result<(), RpcError> {
        terminal_println!("test.go action invoked");
        Ok(())
    }
}

/// The table every simulated device answers. The standard table comes first,
/// then the board's entries, then the captures and `test.drop`, which no
/// entry answers and so come back to the board as calls.
fn table(cli: &SimulateCli) -> &'static [RpcSpec] {
    let mut specs = STANDARD.to_vec();
    Board::new(cli).entries(&mut |entry| specs.push(entry.spec()));
    for shape in &SHAPES {
        specs.push(
            RpcSpec::std(shape.rpc, Access::READ)
                .with_extra_meta((RpcMetaFlags::READABLE | RpcMetaFlags::CAPTURE).bits()),
        );
    }
    specs.push(RpcSpec::std("test.drop", Access::WRITE));
    // Each device makes its table once and keeps it for the whole run.
    Box::leak(specs.into_boxed_slice())
}

/// What one capture RPC serves, whatever a capture of it holds.
struct Shape {
    rpc: &'static str,
    name: &'static str,
    units: &'static str,
    data_type: DataType,
    /// What takes a raw element to `units`.
    y_calibration: f32,
    /// How long after its trigger a capture is ready.
    delay_ns: u64,
    /// Whether the terminal hears of each capture. A capture a host repeats
    /// many times a second says nothing.
    logged: bool,
}

/// The captures, `test.capture` first.
static SHAPES: [Shape; 3] = [
    Shape {
        rpc: "test.capture",
        name: "Test Signal",
        units: "V",
        data_type: DataType::F32,
        y_calibration: 1.0,
        delay_ns: CAPTURE_DELAY_NS,
        logged: true,
    },
    Shape {
        rpc: "fid.capture",
        name: "FID",
        units: "V",
        data_type: DataType::I16,
        y_calibration: FID_VOLTS_PER_COUNT as f32,
        delay_ns: FID_DELAY_NS,
        logged: false,
    },
    Shape {
        rpc: "phase.capture",
        name: "Unwrapped phase",
        units: "rad",
        data_type: DataType::F32,
        y_calibration: 1.0,
        delay_ns: FID_DELAY_NS,
        logged: false,
    },
];
const TEST_CAPTURE: usize = 0;
const FID_CAPTURE: usize = 1;
const PHASE_CAPTURE: usize = 2;

/// The buffer one capture RPC serves.
struct Captured {
    shape: &'static Shape,
    status: Status,
    data: Vec<u8>,
    length: u32,
    x_offset: f32,
    x_stride: f32,
    /// When a capture in progress is ready.
    ready_ns: Option<u64>,
}

impl Captured {
    fn idle(shape: &'static Shape) -> Self {
        Self {
            shape,
            status: Status::Idle,
            data: Vec::new(),
            length: 0,
            x_offset: 0.0,
            x_stride: 0.0,
            ready_ns: None,
        }
    }

    fn served(&self) -> Capture<'_> {
        Capture {
            status: self.status,
            data: &self.data,
            metadata: CaptureMetadata {
                version: METADATA_VERSION,
                data_type: self.shape.data_type,
                data_size: self.data.len() as u32,
                block_size: CAPTURE_BLOCK_SIZE,
                length: self.length,
                y_calibration: self.shape.y_calibration,
                x_offset: self.x_offset,
                x_stride: self.x_stride,
                name: self.shape.name,
                units: self.shape.units,
                x_name: "Time",
                x_units: "s",
            },
        }
    }
}

/// Where one sample clock is in the acquisition.
struct Clock {
    rate: u32,
    /// Samples taken or dropped since the acquisition started.
    taken: u64,
    /// The sample a random drop skips next.
    drop_at: u64,
    /// The sample the next segment starts at.
    rollover_at: u64,
}

impl Clock {
    fn stopped(rate: u32) -> Self {
        Self {
            rate,
            taken: 0,
            drop_at: u64::MAX,
            rollover_at: u64::MAX,
        }
    }

    /// Samples due `elapsed_ns` into the acquisition.
    fn due(&self, elapsed_ns: u64) -> u64 {
        (u128::from(elapsed_ns) * u128::from(self.rate) / 1_000_000_000) as u64
    }
}

/// How far the precession frequency is from `fid.frequency`, as a part of
/// it, and when it was last moved.
struct Wander {
    offset: f64,
    at_ns: u64,
}

/// One simulated device, its board's waveforms and capture buffers around a [`Sim`] on one port.
pub(super) struct SimDevice {
    pub(super) route: DeviceRoute,
    sim: Sim<Board, SEGMENTS>,
    segment_seconds: u32,
    no_drop: bool,
    /// When the acquisition the clocks follow started, while one runs.
    started_ns: Option<u64>,
    main: Clock,
    aux: Clock,
    rng: GaussianRng,
    /// Each capture RPC's buffer, in the order of [`SHAPES`].
    captures: [Captured; 3],
    wander: Wander,
    next_log_ns: u64,
    next_log_level: usize,
}

impl SimDevice {
    /// The device at `route`, `index` in its layout, come up at `now_ns`.
    /// `index` gives it a serial, a session and noise apart from the other
    /// devices.
    pub(super) fn new(cli: &SimulateCli, route: DeviceRoute, index: usize, now_ns: u64) -> Self {
        let seed = unix_duration().as_nanos() as u64
            ^ u64::from(cli.port).rotate_left(32)
            ^ (index as u64).rotate_left(48);
        let mut rng = GaussianRng::new(seed | 1);
        let serial = match index {
            0 => DEVICE_SERIAL.to_string(),
            _ => format!("SIM{:04}", index + 1),
        };
        let identity = Identity::new(DEVICE_NAME, DEVICE_DESC, &serial, DEVICE_FIRMWARE)
            .expect("the simulator's identity fits");
        let mut sim = Sim::new(
            identity,
            next_session(&mut rng),
            table(cli),
            Board::new(cli),
            PASSWORD,
            now_ns,
            vec![Port::new(PORT_CAPACITY)],
        );
        sim.offset = unix_duration().as_secs() as i64 - (now_ns / 1_000_000_000) as i64;
        sim.slot = Some(match cli.upload {
            UploadKind::Image => Slot::Image {
                envelope: CHUNK_ENVELOPE,
                uploaded: 0,
            },
            UploadKind::Signed => Slot::Signed(PACKAGE, Upload::new()),
        });
        sim.upgraded = Identity::new(DEVICE_NAME, UPGRADED_DESC, &serial, DEVICE_FIRMWARE);
        let next_log_ns = now_ns + next_log_delay_ns(&mut rng);
        let mut device = Self {
            route,
            sim,
            segment_seconds: cli.segment_seconds,
            no_drop: cli.no_drop,
            started_ns: None,
            main: Clock::stopped(cli.samplerate),
            aux: Clock::stopped(AUX_SAMPLE_RATE),
            rng,
            captures: SHAPES.each_ref().map(Captured::idle),
            wander: Wander {
                offset: 0.0,
                at_ns: now_ns,
            },
            next_log_ns,
            next_log_level: 0,
        };
        // Firmware waits `dev.autostart` seconds before it acquires; the
        // simulator acquires from boot.
        let _ = device.sim.start(now_ns);
        device.follow();
        device
    }

    #[cfg(test)]
    pub(super) fn serial(&self) -> &str {
        &self.sim.device.identity.serial
    }

    #[cfg(test)]
    pub(super) fn session(&self) -> SessionId {
        self.sim.device.session
    }

    pub(super) fn board(&self) -> &Board {
        self.sim.device.settings()
    }

    /// Sends what a host that just connected needs to know.
    pub(super) fn connected(&mut self, now_ns: u64, out: &mut impl Sink) {
        self.sim.connect(0, now_ns);
        self.drain(out);
    }

    /// Answers a packet addressed to this device, and the board answers what no entry does.
    pub(super) fn handle(&mut self, packet: &[u8], now_ns: u64, out: &mut impl Sink) {
        let session = self.sim.device.session;
        if let Some(call) = self.sim.request(0, packet, now_ns) {
            let capture = SHAPES.iter().position(|shape| shape.rpc == call.name);
            let answered = match (capture, call.name) {
                (Some(which), _) => self.capture_rpc(which, &call.args, now_ns),
                (None, "test.drop") => self.drop_rpc(&call.args),
                (None, _) => Err(RpcError::NotFound),
            };
            self.sim
                .answer(&call, answered.as_deref().map_err(|&error| error));
        }
        if self.sim.device.session != session {
            self.booted(now_ns);
        }
        self.follow();
        self.drain(out);
    }

    /// Sends the host what the port holds, as firmware egress does.
    fn drain(&mut self, out: &mut impl Sink) {
        while let Some(packet) = self.sim.ports[0].egress() {
            out.send(&packet);
        }
    }

    /// Puts the clocks at sample zero of an acquisition that started since they last looked.
    fn follow(&mut self) {
        if self.sim.started() != self.started_ns {
            self.started_ns = self.sim.started();
            self.main = self.started(self.main.rate);
            self.aux = self.started(self.aux.rate);
        }
    }

    /// Answers capture `which` for the selector `args` encodes.
    fn capture_rpc(&mut self, which: usize, args: &[u8], now_ns: u64) -> Result<Vec<u8>, RpcError> {
        self.update_captures(now_ns);
        let selector = Selector::parse(args)?;
        let mut reply = Reply::new();
        self.captures[which].served().reply(selector, &mut reply)?;
        if selector == Selector::Trigger {
            self.begin_capture(which, now_ns);
        }
        Ok(reply.to_vec())
    }

    /// Takes capture `which` as the board stands, ready after its delay.
    fn begin_capture(&mut self, which: usize, now_ns: u64) {
        let shape = &SHAPES[which];
        let (data, length, x_offset, x_stride) = match which {
            TEST_CAPTURE => self.sine_capture(),
            FID_CAPTURE => {
                let frequency = self.precession(now_ns);
                let decay = self.sim.device.settings().decay();
                let data = fid_shot(frequency, decay, &mut self.rng);
                (data, FID_SAMPLES, 0.0, 1.0 / FID_SAMPLE_RATE)
            }
            PHASE_CAPTURE => {
                let frequency = self.precession(now_ns);
                let decay = self.sim.device.settings().decay();
                let data = phase_shot(frequency, decay, &mut self.rng);
                (data, FID_SAMPLES, 0.0, 1.0 / FID_SAMPLE_RATE)
            }
            _ => unreachable!("a capture is one of SHAPES"),
        };
        if shape.logged {
            terminal_println!(
                "{}: {} triggered ({length} samples); data available in ~{:.1}s",
                self.route,
                shape.rpc,
                shape.delay_ns as f64 / 1e9
            );
        }
        self.captures[which] = Captured {
            shape,
            status: Status::Capturing,
            data,
            length: length as u32,
            x_offset: x_offset as f32,
            x_stride: x_stride as f32,
            ready_ns: Some(now_ns + shape.delay_ns),
        };
    }

    /// The sine as it streams from the sample the main clock is at, as
    /// `f32`, with its count, the time of its first sample and the time
    /// between two.
    fn sine_capture(&mut self) -> (Vec<u8>, usize, f64, f64) {
        let span = CAPTURE_SAMPLES_MAX - CAPTURE_SAMPLES_MIN + 1;
        let count = CAPTURE_SAMPLES_MIN + (self.rng.next_u64() as usize % span);
        let rate = f64::from(self.main.rate);
        let board = self.sim.device.settings();
        let (amplitude, frequency) = (board.amplitude(), board.frequency());
        let noise_sigma = board.noise() * (rate / 2.0).sqrt();
        let start = self.main.taken;
        let mut data = Vec::with_capacity(count * std::mem::size_of::<f32>());
        for offset in 0..count as u64 {
            let t = (start + offset) as f64 / rate;
            let phase = std::f64::consts::TAU * frequency * t;
            let value = amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
            data.extend((value as f32).to_le_bytes());
        }
        (data, count, start as f64 / rate, 1.0 / rate)
    }

    /// The precession frequency at `now_ns`, in Hz. It wanders about
    /// `fid.frequency` by [`FID_WANDER`] of it, and forgets where it was over
    /// [`FID_WANDER_SECONDS`]. Asked twice at one instant it answers the same.
    fn precession(&mut self, now_ns: u64) -> f64 {
        let elapsed = now_ns.saturating_sub(self.wander.at_ns) as f64 / 1e9;
        if elapsed > 0.0 {
            let kept = (-elapsed / FID_WANDER_SECONDS).exp();
            let moved = FID_WANDER * (1.0 - kept * kept).sqrt() * self.rng.next_gaussian();
            self.wander.offset = self.wander.offset * kept + moved;
            self.wander.at_ns = now_ns;
        }
        self.sim.device.settings().fid_frequency() * (1.0 + self.wander.offset)
    }

    /// Ends each capture whose delay has passed.
    fn update_captures(&mut self, now_ns: u64) {
        for capture in &mut self.captures {
            if capture.ready_ns.is_some_and(|at| now_ns >= at) {
                capture.ready_ns = None;
                capture.status = Status::Done;
                if capture.shape.logged {
                    terminal_println!(
                        "{}: {} done ({} bytes)",
                        self.route,
                        capture.shape.rpc,
                        capture.data.len()
                    );
                }
            }
        }
    }

    /// A clock at sample zero, with its first drop and rollover.
    fn started(&mut self, rate: u32) -> Clock {
        Clock {
            rate,
            taken: 0,
            drop_at: self.next_drop_after(0, rate),
            rollover_at: u64::from(rate) * u64::from(self.segment_seconds),
        }
    }

    /// Power-cycles into a new session. The board's entries go back to their
    /// defaults and then to what is stored, and the acquisition starts afresh.
    pub(super) fn reboot(&mut self, now_ns: u64, out: &mut impl Sink) {
        let session = SessionId::new(self.sim.device.session.value().wrapping_add(1));
        self.sim.reboot(session, now_ns);
        self.booted(now_ns);
        self.follow();
        self.drain(out);
    }

    /// Comes up as the board does, with an idle capture, an acquisition from boot and the records
    /// a host needs.
    fn booted(&mut self, now_ns: u64) {
        self.captures = SHAPES.each_ref().map(Captured::idle);
        let _ = self.sim.start(now_ns);
        terminal_println!(
            "{}: rebooted; new session id {}",
            self.route,
            self.sim.device.session.value()
        );
        self.sim.connect(0, now_ns);
    }

    /// Drops the next sample of each clock, as a random drop does.
    pub(super) fn drop_samples_now(&mut self) {
        self.main.drop_at = self.main.taken;
        self.aux.drop_at = self.aux.taken;
    }

    /// Answers `test.drop`. With no argument it drops the next sample of each
    /// clock, as the `d` key does. With `rollover` it drops the first sample
    /// of each clock's next segment.
    fn drop_rpc(&mut self, args: &[u8]) -> Result<Vec<u8>, RpcError> {
        match args {
            b"" => self.drop_samples_now(),
            b"rollover" => {
                self.main.drop_at = self.main.rollover_at;
                self.aux.drop_at = self.aux.rollover_at;
            }
            _ => return Err(RpcError::Invalid),
        }
        Ok(Vec::new())
    }

    /// Sends the heartbeat, log message and samples due at `now_ns`. A configuration left unsaved long
    /// enough is stored, as firmware's autosave does.
    pub(super) fn tick(&mut self, now_ns: u64, out: &mut impl Sink) {
        self.sim.tick(now_ns);
        self.follow();
        self.update_captures(now_ns);
        self.log_if_due(now_ns);
        let Some(started_ns) = self.started_ns else {
            return;
        };
        let elapsed_ns = now_ns.saturating_sub(started_ns);
        let (main_due, aux_due) = (self.main.due(elapsed_ns), self.aux.due(elapsed_ns));
        while self.main.taken < main_due {
            self.sample_main();
        }
        while self.aux.taken < aux_due {
            self.sample_aux();
        }
        self.sim.flush();
        self.drain(out);
    }

    /// Takes the next sine and status sample.
    fn sample_main(&mut self) {
        if self.main.taken == self.main.rollover_at {
            self.main.rollover_at += u64::from(self.main.rate) * u64::from(self.segment_seconds);
            self.sim.rollover(0);
            self.sim.rollover(1);
        }
        if self.main.taken == self.main.drop_at {
            terminal_println!(
                "{}: dropped sample {} of streams 1 and 2",
                self.route,
                self.main.taken
            );
            self.sim.skip(0, 1);
            self.sim.skip(1, 1);
            self.main.taken += 1;
            self.main.drop_at = self.next_drop_after(self.main.taken, self.main.rate);
            return;
        }
        let rate = f64::from(self.main.rate);
        let board = self.sim.device.settings();
        let (amplitude, frequency) = (board.amplitude(), board.frequency());
        let noise_sigma = board.noise() * (rate / 2.0).sqrt();
        let status = board.status();
        let phase = std::f64::consts::TAU * frequency * (self.main.taken as f64 / rate);
        let sine = amplitude * phase.sin() + noise_sigma * self.rng.next_gaussian();
        let cosine = amplitude * phase.cos() + noise_sigma * self.rng.next_gaussian();
        let mut sample = [0u8; 16];
        sample[..8].copy_from_slice(&sine.to_le_bytes());
        sample[8..].copy_from_slice(&cosine.to_le_bytes());
        self.sim.push(0, &sample);
        self.sim.push(1, &[status, SIGNAL_LEVEL]);
        self.main.taken += 1;
    }

    /// Takes the next aux sample.
    fn sample_aux(&mut self) {
        if self.aux.taken == self.aux.rollover_at {
            self.aux.rollover_at += u64::from(self.aux.rate) * u64::from(self.segment_seconds);
            self.sim.rollover(2);
        }
        if self.aux.taken == self.aux.drop_at {
            terminal_println!(
                "{}: dropped sample {} of stream 3",
                self.route,
                self.aux.taken
            );
            self.sim.skip(2, 1);
            self.aux.taken += 1;
            self.aux.drop_at = self.next_drop_after(self.aux.taken, self.aux.rate);
            return;
        }
        let cycles = AUX_WAVE_FREQUENCY * self.aux.taken as f64 / f64::from(self.aux.rate);
        let phase = cycles.fract();
        let triangle = 1.0 - 4.0 * (phase - 0.5).abs();
        let sawtooth = 2.0 * phase - 1.0;
        let mut sample = [0u8; 16];
        sample[..8].copy_from_slice(&triangle.to_le_bytes());
        sample[8..].copy_from_slice(&sawtooth.to_le_bytes());
        self.sim.push(2, &sample);
        self.aux.taken += 1;
    }

    fn next_drop_after(&mut self, current: u64, rate: u32) -> u64 {
        if self.no_drop {
            return u64::MAX;
        }
        let min_seconds = SAMPLE_DROP_INTERVAL_SECONDS - SAMPLE_DROP_JITTER_SECONDS;
        let seconds = min_seconds + self.rng.next_unit() * SAMPLE_DROP_JITTER_SECONDS * 2.0;
        let interval = (seconds * f64::from(rate)).round().max(1.0) as u64;
        current.saturating_add(interval)
    }

    /// Sends a log message once one is due, cycling through the levels. One
    /// beneath `dev.loglevel` goes unsent, as firmware leaves it.
    fn log_if_due(&mut self, now_ns: u64) {
        if now_ns < self.next_log_ns {
            return;
        }
        let levels = [
            LogLevel::CRITICAL,
            LogLevel::ERROR,
            LogLevel::WARNING,
            LogLevel::INFO,
            LogLevel::DEBUG,
        ];
        let level = levels[self.next_log_level % levels.len()];
        self.next_log_level = self.next_log_level.wrapping_add(1);
        let lucky = (self.rng.next_u64() % 10_000) as u32;
        let templates = [
            "lucky number {lucky} nudged the simulated flux loop",
            "telemetry monitor reported lucky number {lucky}",
            "calibration check landed on lucky number {lucky}",
            "simulated event counter reached lucky number {lucky}",
            "operator marker recorded lucky number {lucky}",
            "background diagnostic index settled at lucky number {lucky}",
        ];
        let template = templates[(self.rng.next_u64() % templates.len() as u64) as usize];
        let message = template.replace("{lucky}", &lucky.to_string());
        self.sim.log(level, lucky, &message);
        self.next_log_ns = now_ns + next_log_delay_ns(&mut self.rng);
    }
}

/// One shot of a free induction decay as a 16 bit ADC reads it out at
/// [`FID_SAMPLE_RATE`], in counts of [`FID_VOLTS_PER_COUNT`]. Sample `i` is
/// `amplitude * exp(-t / t2) * sin(2 pi frequency t)` and noise, at
/// `t = i / FID_SAMPLE_RATE`, and a value past the converter's range reads as
/// its end.
fn fid_shot(frequency: f64, decay: Decay, rng: &mut GaussianRng) -> Vec<u8> {
    let mut data = Vec::with_capacity(FID_SAMPLES * std::mem::size_of::<i16>());
    for index in 0..FID_SAMPLES {
        let t = index as f64 / FID_SAMPLE_RATE;
        let phase = std::f64::consts::TAU * frequency * t;
        let volts = decay.envelope(t) * phase.sin() + decay.noise * rng.next_gaussian();
        let counts = (volts / FID_VOLTS_PER_COUNT).round();
        let counts = counts.clamp(f64::from(i16::MIN), f64::from(i16::MAX)) as i16;
        data.extend(counts.to_le_bytes());
    }
    data
}

/// The unwrapped phase of a shot of [`fid_shot`]'s kind, in radians as
/// `f32`. Sample `i` is `2 pi frequency t` and the noise the shot's own noise
/// leaves in its phase, which grows as the envelope falls and is at most a
/// half turn.
fn phase_shot(frequency: f64, decay: Decay, rng: &mut GaussianRng) -> Vec<u8> {
    let mut data = Vec::with_capacity(FID_SAMPLES * std::mem::size_of::<f32>());
    for index in 0..FID_SAMPLES {
        let t = index as f64 / FID_SAMPLE_RATE;
        let phase = std::f64::consts::TAU * frequency * t;
        let sigma = match decay.noise / decay.envelope(t) {
            sigma if sigma < std::f64::consts::PI => sigma,
            _ => std::f64::consts::PI,
        };
        let noise = match decay.noise > 0.0 {
            true => sigma * rng.next_gaussian(),
            false => 0.0,
        };
        data.extend(((phase + noise) as f32).to_le_bytes());
    }
    data
}

fn next_session(rng: &mut GaussianRng) -> SessionId {
    SessionId::new(
        (rng.next_u64() as u32)
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223),
    )
}

fn next_log_delay_ns(rng: &mut GaussianRng) -> u64 {
    LOG_MESSAGE_MIN_INTERVAL_NS + (LOG_MESSAGE_JITTER_NS as f64 * rng.next_unit()) as u64
}

#[cfg(test)]
mod tests {
    use super::super::tree::Queue;
    use super::*;
    use clap::Parser;
    use std::collections::BTreeMap;
    use twinleaf::data::{Boundary, Change, PacketParser};
    use twinleaf::tio::packet::{Packet, Payload};

    const TICK_NS: u64 = 100_000_000;

    /// One simulated device, driven in process on a clock of the test's own,
    /// and a parser that reads what it sends.
    struct Driven {
        sim: SimDevice,
        now_ns: u64,
        parser: PacketParser,
        /// Each stream's batches, as their first timestamp, their last, and
        /// their boundary.
        batches: BTreeMap<u8, Vec<(f64, f64, Option<Boundary>)>>,
    }

    /// One capture as a host reads it, its values calibrated.
    struct Read {
        name: String,
        units: String,
        x: (String, String),
        data_type: DataType,
        x_offset: f32,
        x_stride: f32,
        y_calibration: f32,
        raw: Vec<u8>,
        values: Vec<f64>,
    }

    impl Driven {
        fn new() -> Driven {
            Driven::with(&[])
        }

        /// The device `args` start, besides a sample rate of 10 Hz and no drops.
        fn with(args: &[&str]) -> Driven {
            let fixed = ["tio-simulate", "--samplerate", "10", "--no-drop"];
            let cli = SimulateCli::parse_from(fixed.iter().chain(args));
            let mut driven = Driven {
                sim: SimDevice::new(&cli, DeviceRoute::root(), 0, 0),
                now_ns: 0,
                parser: PacketParser::new(false),
                batches: BTreeMap::new(),
            };
            let mut out = Queue::default();
            driven.sim.connected(0, &mut out);
            driven.parse(out);
            driven
        }

        fn parse(&mut self, out: Queue) {
            for raw in out.0 {
                let (packet, _) = Packet::from_slice_prefix(&raw).unwrap();
                self.parser.push_packet(&packet).unwrap();
                while let Some(batch) = self.parser.pop_batch() {
                    let times = batch.timestamps();
                    self.batches
                        .entry(batch.stream_key().stream_id.value())
                        .or_default()
                        .push((times[0], times[times.len() - 1], batch.boundary().cloned()));
                }
            }
        }

        /// Runs the device for `seconds` of its clock.
        fn run(&mut self, seconds: u64) {
            for _ in 0..seconds * 1_000_000_000 / TICK_NS {
                self.now_ns += TICK_NS;
                let mut out = Queue::default();
                self.sim.tick(self.now_ns, &mut out);
                self.parse(out);
            }
        }

        /// Asks for `name` with no argument.
        fn ask(&mut self, name: &str) {
            let request = Packet::rpc_request(name, &[], 1, DeviceRoute::root()).unwrap();
            let mut out = Queue::default();
            self.sim.handle(request.as_bytes(), self.now_ns, &mut out);
            self.parse(out);
        }

        /// Calls `name` with `args` and returns its reply or its error.
        fn call(&mut self, name: &str, args: &[u8]) -> Result<Vec<u8>, RpcError> {
            let request = Packet::rpc_request(name, args, 7, DeviceRoute::root()).unwrap();
            let mut out = Queue::default();
            self.sim.handle(request.as_bytes(), self.now_ns, &mut out);
            let answered = out.0.iter().find_map(|raw| {
                let (packet, _) = Packet::from_slice_prefix(raw).unwrap();
                match packet.payload() {
                    Payload::RpcReply(reply) => Some(Ok(reply.value.to_vec())),
                    Payload::RpcError(error) => Some(Err(RpcError::from(error.code))),
                    _ => None,
                }
            });
            self.parse(out);
            answered.expect("an RPC is answered")
        }

        /// Calls capture `rpc` with `selector`.
        fn select(&mut self, rpc: &str, selector: i16) -> Result<Vec<u8>, RpcError> {
            self.call(rpc, &selector.to_le_bytes())
        }

        /// Triggers capture `rpc` now, and reads it once its delay has
        /// passed on the device's clock.
        fn capture(&mut self, rpc: &str) -> Read {
            self.select(rpc, -1).unwrap();
            self.read(rpc)
        }

        /// Reads the capture `rpc` was triggered for, which is busy until
        /// one repetition has passed.
        fn read(&mut self, rpc: &str) -> Read {
            assert_eq!(self.select(rpc, -2), Ok(vec![1]), "{rpc} is capturing");
            assert_eq!(self.select(rpc, 0), Err(RpcError::Busy));
            assert_eq!(self.select(rpc, -1), Err(RpcError::Busy));
            self.now_ns += FID_DELAY_NS;
            self.collect(rpc)
        }

        /// Reads the capture `rpc` has done.
        fn collect(&mut self, rpc: &str) -> Read {
            assert_eq!(self.select(rpc, -2), Ok(vec![2]), "{rpc} is done");
            let reply = self.select(rpc, -3).unwrap();
            let meta = CaptureMetadata::parse(&reply).unwrap();
            let mut raw = Vec::new();
            let blocks = (meta.data_size as usize).div_ceil(usize::from(meta.block_size));
            for block in 0..blocks {
                raw.extend(self.select(rpc, block as i16).unwrap());
            }
            assert_eq!(raw.len(), meta.data_size as usize);
            assert_eq!(
                self.select(rpc, blocks as i16),
                Err(RpcError::Invalid),
                "no block past the last"
            );
            let scale = f64::from(meta.y_calibration);
            let values: Vec<f64> = match meta.data_type {
                DataType::I16 => counts_of(&raw)
                    .map(|counts| f64::from(counts) * scale)
                    .collect(),
                DataType::F32 => raw
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|bytes| f64::from(f32::from_le_bytes(*bytes)) * scale)
                    .collect(),
                other => panic!("{rpc} is of {other:?}"),
            };
            assert_eq!(values.len(), meta.length as usize);
            Read {
                name: meta.name.to_string(),
                units: meta.units.to_string(),
                x: (meta.x_name.to_string(), meta.x_units.to_string()),
                data_type: meta.data_type,
                x_offset: meta.x_offset,
                x_stride: meta.x_stride,
                y_calibration: meta.y_calibration,
                raw,
                values,
            }
        }
    }

    /// The counts a capture of a 16 bit ADC holds.
    fn counts_of(raw: &[u8]) -> impl Iterator<Item = i16> + '_ {
        raw.as_chunks::<2>()
            .0
            .iter()
            .map(|bytes| i16::from_le_bytes(*bytes))
    }

    /// The root mean square of `values`.
    fn rms(values: &[f64]) -> f64 {
        (values.iter().map(|value| value * value).sum::<f64>() / values.len() as f64).sqrt()
    }

    /// The precession frequency a noiseless phase capture read, from its
    /// last sample.
    fn frequency_of(phase: &Read) -> f64 {
        let last = phase.values.len() - 1;
        phase.values[last] / (std::f64::consts::TAU * last as f64 / FID_SAMPLE_RATE)
    }

    /// An FID capture is 1500 samples of a 16 bit ADC, 750 us at 2 MS/s
    /// from the start of the shot, calibrated to volts. It is busy for one
    /// repetition after its trigger, and its envelope falls as T2 says.
    #[test]
    fn an_fid_capture_is_one_shot_as_its_adc_reads_it_out() {
        let mut driven = Driven::new();
        assert_eq!(driven.select("fid.capture", -2), Ok(vec![0]), "idle");
        let fid = driven.capture("fid.capture");

        assert_eq!((fid.name.as_str(), fid.units.as_str()), ("FID", "V"));
        assert_eq!(fid.x, ("Time".to_string(), "s".to_string()));
        assert_eq!(fid.data_type, DataType::I16);
        assert_eq!(fid.values.len(), 1500);
        assert_eq!((fid.x_offset, fid.x_stride), (0.0, 0.5e-6));
        assert_eq!(fid.y_calibration, (2.5 / 32768.0) as f32);
        // A sine of 1 V whose envelope falls to 1/e each 250 us: its first
        // tenth averages 0.61 V rms and its last 0.041 V, over 0.01 V of noise.
        let (early, late) = (rms(&fid.values[..150]), rms(&fid.values[1350..]));
        assert!((0.55..0.67).contains(&early), "{early}");
        assert!((0.03..0.055).contains(&late), "{late}");
    }

    /// With no noise each sample of an FID is its formula in counts, and a
    /// phase capture taken at the same instant is that formula's phase. A
    /// shot taken later precesses at another frequency, within a few parts
    /// in a thousand of `fid.frequency`.
    #[test]
    fn a_shot_is_its_formula_and_the_next_precesses_at_another_frequency() {
        let mut driven = Driven::with(&["--fid-noise", "0"]);
        let mut frequencies = Vec::new();
        for shot in 0..3 {
            driven.select("fid.capture", -1).unwrap();
            driven.select("phase.capture", -1).unwrap();
            let fid = driven.read("fid.capture");
            let phase = driven.collect("phase.capture");
            let frequency = frequency_of(&phase);
            for (index, counts) in counts_of(&fid.raw).enumerate() {
                let t = index as f64 / FID_SAMPLE_RATE;
                let volts = (-t / 250e-6).exp() * (std::f64::consts::TAU * frequency * t).sin();
                let expected = volts / FID_VOLTS_PER_COUNT;
                let counts = f64::from(counts);
                // The phase is an `f32`, so the frequency read from it is
                // off by parts in ten million, a few counts at the end.
                assert!(
                    (counts - expected).abs() < 8.0,
                    "shot {shot} sample {index}: {counts} for {expected}"
                );
            }
            assert!(
                (frequency / 175_000.0 - 1.0).abs() < 6e-3,
                "shot {shot} precesses at {frequency} Hz"
            );
            frequencies.push(frequency);
            driven.run(1);
        }
        assert!((frequencies[0] - 175_000.0).abs() < 0.1, "{frequencies:?}");
        assert!(
            (frequencies[1] - frequencies[0]).abs() > 1.0,
            "{frequencies:?}"
        );
        assert!(
            (frequencies[2] - frequencies[1]).abs() > 1.0,
            "{frequencies:?}"
        );
    }

    /// A phase capture is the shot's unwrapped phase in radians, on the
    /// FID's own x, a line whose noise grows as the envelope falls.
    #[test]
    fn a_phase_capture_is_a_line_that_grows_noisier_as_the_shot_decays() {
        let mut driven = Driven::new();
        let phase = driven.capture("phase.capture");

        assert_eq!(phase.name, "Unwrapped phase");
        assert_eq!(phase.units, "rad");
        assert_eq!(phase.x, ("Time".to_string(), "s".to_string()));
        assert_eq!(phase.data_type, DataType::F32);
        assert_eq!(phase.values.len(), 1500);
        assert_eq!((phase.x_offset, phase.x_stride), (0.0, 0.5e-6));
        let residual = |range: std::ops::Range<usize>| {
            let off: Vec<f64> = range
                .map(|index| {
                    let t = index as f64 / FID_SAMPLE_RATE;
                    phase.values[index] - std::f64::consts::TAU * 175_000.0 * t
                })
                .collect();
            rms(&off)
        };
        // 0.01 V of noise on an envelope of 1 V at first and 0.05 V at last.
        let (early, late) = (residual(0..150), residual(1350..1500));
        assert!((0.008..0.016).contains(&early), "{early}");
        assert!((0.12..0.3).contains(&late), "{late}");
    }

    /// A shot larger than the converter's range reads as the range's ends.
    #[test]
    fn an_fid_past_the_converters_range_reads_as_its_ends() {
        let mut driven = Driven::with(&["--fid-amplitude", "10", "--fid-noise", "0"]);
        let fid = driven.capture("fid.capture");
        let counts: Vec<i16> = counts_of(&fid.raw).collect();
        assert_eq!(counts.iter().max(), Some(&i16::MAX));
        assert_eq!(counts.iter().min(), Some(&i16::MIN));
    }

    /// The decay's entries set the next shot, and refuse what no decay has.
    #[test]
    fn the_fid_entries_set_the_next_shot() {
        let mut driven = Driven::with(&["--fid-noise", "0"]);
        assert_eq!(
            driven.call("fid.t2", &0f64.to_le_bytes()),
            Err(RpcError::Range)
        );
        assert_eq!(
            driven.call("fid.amplitude", &(-1f64).to_le_bytes()),
            Err(RpcError::Range)
        );
        driven.call("fid.amplitude", &0.5f64.to_le_bytes()).unwrap();
        driven.call("fid.t2", &1f64.to_le_bytes()).unwrap();
        driven
            .call("fid.frequency", &1000f64.to_le_bytes())
            .unwrap();
        let fid = driven.capture("fid.capture");
        // Half a volt that barely decays, three quarters of a 1 kHz cycle.
        let peak = fid.values.iter().cloned().fold(f64::MIN, f64::max);
        assert!((peak - 0.5).abs() < 1e-3, "{peak}");
        assert!(
            (fid.values[1499] + 0.5).abs() < 2e-3,
            "{}",
            fid.values[1499]
        );
    }

    /// A capture of one RPC leaves the others as they were.
    #[test]
    fn each_capture_has_a_buffer_of_its_own() {
        let mut driven = Driven::new();
        driven.capture("fid.capture");
        assert_eq!(driven.select("phase.capture", -2), Ok(vec![0]));
        assert_eq!(driven.select("test.capture", -2), Ok(vec![0]));
        driven.select("test.capture", -1).unwrap();
        assert_eq!(driven.select("fid.capture", -2), Ok(vec![2]));
        assert_eq!(
            driven.select("fid.capture", 0).map(|block| block.len()),
            Ok(256)
        );
    }

    /// A restart, by one `dev.restart` or by `dev.stop` and then `dev.start`,
    /// reaches a host as a restart of every stream with no loss, and the new
    /// acquisition's samples come after the last of the one before.
    #[test]
    fn a_restart_reaches_a_host_with_no_loss_and_goes_on_in_time() {
        for asks in [&["dev.restart"][..], &["dev.stop", "dev.start"]] {
            let mut driven = Driven::new();
            driven.run(3);
            for name in asks {
                driven.ask(name);
                driven.run(1);
            }
            driven.run(2);
            assert_eq!(driven.batches.len(), 3, "{asks:?}");
            for (stream, batches) in &driven.batches {
                let boundaries: Vec<_> = batches
                    .iter()
                    .enumerate()
                    .filter_map(|(at, (_, _, boundary))| Some((at, boundary.clone()?)))
                    .collect();
                let [(0, initial), (at, restart)] = &boundaries[..] else {
                    panic!("{asks:?} stream {stream} reports {boundaries:?}");
                };
                assert_eq!(initial.change, Some(Change::Initial));
                assert!(
                    matches!(restart.change, Some(Change::Restarted { .. })),
                    "{asks:?} stream {stream} reports {restart:?}"
                );
                assert_eq!(restart.loss, None, "{asks:?} stream {stream}");
                assert!(
                    batches[*at].0 > batches[at - 1].1,
                    "{asks:?} stream {stream} goes back in time"
                );
            }
        }
    }
}

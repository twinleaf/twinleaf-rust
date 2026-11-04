//! One simulated device. The `twinleaf-device` machine answers its packets
//! and streams its samples, and the board around it supplies the rest. That
//! is the waveforms, the capture buffer, the stored configuration and the
//! firmware slot, all in memory.

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
    samplerate: u32,
}

/// Refuses a value that is negative or not finite.
fn nonnegative(value: f64) -> Result<f64, RpcError> {
    match value.is_finite() && value >= 0.0 {
        true => Ok(value),
        false => Err(RpcError::Range),
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
            samplerate: cli.samplerate,
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
/// then the board's entries, then `test.capture` and `test.drop`, which no
/// entry answers and so come back to the board as calls.
fn table(cli: &SimulateCli) -> &'static [RpcSpec] {
    let mut specs = STANDARD.to_vec();
    Board::new(cli).entries(&mut |entry| specs.push(entry.spec()));
    specs.push(
        RpcSpec::std("test.capture", Access::READ)
            .with_extra_meta((RpcMetaFlags::READABLE | RpcMetaFlags::CAPTURE).bits()),
    );
    specs.push(RpcSpec::std("test.drop", Access::WRITE));
    // Each device makes its table once and keeps it for the whole run.
    Box::leak(specs.into_boxed_slice())
}

/// The capture buffer `test.capture` serves.
struct Captured {
    status: Status,
    data: Vec<u8>,
    length: u32,
    x_offset: f32,
    x_stride: f32,
    /// When a capture in progress is ready.
    ready_ns: Option<u64>,
}

impl Captured {
    fn idle() -> Self {
        Self {
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
                data_type: DataType::F32,
                data_size: self.data.len() as u32,
                block_size: CAPTURE_BLOCK_SIZE,
                length: self.length,
                y_calibration: 1.0,
                x_offset: self.x_offset,
                x_stride: self.x_stride,
                name: "Test Signal",
                units: "V",
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

/// One simulated device, its board's waveforms and capture buffer around a [`Sim`] on one port.
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
    capture: Captured,
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
            capture: Captured::idle(),
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
            let answered = match call.name {
                "test.capture" => self.capture_rpc(&call.args, now_ns),
                "test.drop" => self.drop_rpc(&call.args),
                _ => Err(RpcError::NotFound),
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

    /// Answers `test.capture` for the selector `args` encodes.
    fn capture_rpc(&mut self, args: &[u8], now_ns: u64) -> Result<Vec<u8>, RpcError> {
        self.update_capture(now_ns);
        let selector = Selector::parse(args)?;
        let mut reply = Reply::new();
        self.capture.served().reply(selector, &mut reply)?;
        if selector == Selector::Trigger {
            self.begin_capture(now_ns);
        }
        Ok(reply.to_vec())
    }

    /// Takes a capture of the sine as it stands, ready after a delay.
    fn begin_capture(&mut self, now_ns: u64) {
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
        terminal_println!(
            "{}: test.capture triggered ({count} samples); data available in ~{:.1}s",
            self.route,
            CAPTURE_DELAY_NS as f64 / 1e9
        );
        self.capture = Captured {
            status: Status::Capturing,
            data,
            length: count as u32,
            x_offset: (start as f64 / rate) as f32,
            x_stride: (1.0 / rate) as f32,
            ready_ns: Some(now_ns + CAPTURE_DELAY_NS),
        };
    }

    /// Ends a capture whose delay has passed.
    fn update_capture(&mut self, now_ns: u64) {
        if self.capture.ready_ns.is_some_and(|at| now_ns >= at) {
            self.capture.ready_ns = None;
            self.capture.status = Status::Done;
            terminal_println!(
                "{}: test.capture done ({} bytes)",
                self.route,
                self.capture.data.len()
            );
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
        self.capture = Captured::idle();
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
        self.update_capture(now_ns);
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
    use twinleaf::tio::packet::Packet;

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

    impl Driven {
        fn new() -> Driven {
            let cli = SimulateCli::parse_from(["tio-simulate", "--samplerate", "10", "--no-drop"]);
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

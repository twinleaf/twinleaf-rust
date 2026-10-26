//! tio simulate
//!
//! Simulates a small Twinleaf device that publishes a noisy sine wave on stream 1.
//! With `--layout`, it simulates one such device at each route given, all
//! behind the one UDP port, as a hub and the devices on its ports are.
//!
//! Each device is the `twinleaf-device` machine firmware runs, stepped by
//! this loop's clock instead of an executor. It answers the standard table,
//! announces its settings, and streams its samples with their segments and
//! metadata as firmware does.

use crate::SimulateCli;
use ratatui::crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use twinleaf::proto::packet::{Packet, PacketView};
use twinleaf::proto::DeviceRoute;

// In raw mode, '\n' does not reliably return the cursor to column 0.
macro_rules! terminal_println {
    ($($arg:tt)*) => {
        $crate::tools::simulate::terminal_print_line(format_args!($($arg)*))
    };
}

macro_rules! terminal_eprintln {
    ($($arg:tt)*) => {
        $crate::tools::simulate::terminal_error_line(format_args!($($arg)*))
    };
}

mod device;
mod tree;

use device::SimDevice;
use tree::{Node, Queue};

pub fn run_simulate(cli: SimulateCli) -> eyre::Result<()> {
    let mut simulator = Simulator::new(&cli)?;
    simulator.run(&cli)?;
    Ok(())
}

const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

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

fn unix_duration() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// The devices of a layout, behind the one UDP port a host reaches them on.
/// It serves one host at a time.
struct Simulator {
    socket: UdpSocket,
    client: Option<Client>,
    tree: Node,
    /// The routes with a device.
    routes: BTreeSet<DeviceRoute>,
    /// The instant the devices count their nanoseconds from.
    booted: Instant,
}

impl Simulator {
    /// Binds the UDP port and boots a device at each route of the layout.
    ///
    /// # Errors
    ///
    /// Returns an error if the port cannot be bound, or the layout gives a
    /// route twice or reaches one through a device's empty port.
    fn new(cli: &SimulateCli) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", cli.port))?;
        socket.set_nonblocking(true)?;
        let booted = Instant::now();
        let mut routes = cli.layout.clone();
        routes.sort();
        let devices = routes
            .iter()
            .enumerate()
            .map(|(index, route)| SimDevice::new(cli, *route, index, 0))
            .collect();
        let mut tree =
            Node::build(devices).map_err(|why| io::Error::new(io::ErrorKind::InvalidInput, why))?;
        // The first heartbeats tell each hub what is plugged into it, before
        // a host asks for anything behind it.
        tree.tick(0, &mut Queue::default());
        Ok(Self {
            socket,
            client: None,
            tree,
            routes: routes.into_iter().collect(),
            booted,
        })
    }

    fn now_ns(&self) -> u64 {
        self.booted.elapsed().as_nanos() as u64
    }

    fn run(&mut self, cli: &SimulateCli) -> io::Result<()> {
        let raw_mode = match RawModeGuard::enable() {
            Ok(guard) => Some(guard),
            Err(err) => {
                terminal_eprintln!("keyboard shortcuts disabled: {err}");
                None
            }
        };

        let port = self.socket.local_addr()?.port();
        terminal_println!("tio test listening on udp://0.0.0.0:{port}");
        match &self.routes.iter().collect::<Vec<_>>()[..] {
            [route] if route.is_empty() => {}
            [route] => terminal_println!("  device at {route}"),
            routes => {
                let routes: Vec<String> = routes.iter().map(|route| route.to_string()).collect();
                terminal_println!("  devices at {}", routes.join(", "));
            }
        }
        let missing: BTreeSet<DeviceRoute> = self
            .routes
            .iter()
            .filter_map(|route| tree::missing_parent(&self.routes, route))
            .collect();
        for parent in missing {
            terminal_println!("  no device at {parent}: it answers nothing and sends no heartbeat");
        }
        self.print_streams(cli);
        if raw_mode.is_some() {
            terminal_println!("  press d to drop one sample now, r to reboot, Ctrl-C to quit");
        }
        terminal_println!("  connect with: tio proxy udp4://127.0.0.1:{port}");

        loop {
            if raw_mode.is_some() && !self.handle_keyboard()? {
                terminal_println!("stopping tio test");
                return Ok(());
            }
            self.receive_packets()?;
            self.expire_client();
            let mut sent = Queue::default();
            self.tree.tick(self.now_ns(), &mut sent);
            self.send(sent)?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Prints what the devices stream. Every device of a layout streams the
    /// same.
    fn print_streams(&self, cli: &SimulateCli) {
        let board = self.tree.devices()[0].board();
        terminal_println!(
            "  stream 1: 2 waveform channels, amplitude={} V frequency={} Hz noise={} V/sqrt(Hz) samplerate={} Hz segment={} s",
            board.amplitude(),
            board.frequency(),
            board.noise(),
            cli.samplerate,
            cli.segment_seconds
        );
        terminal_println!(
            "  stream 2: status={} signal_level={}",
            board.status(),
            device::SIGNAL_LEVEL
        );
        terminal_println!(
            "  stream 3: aux triangle/sawtooth at {} Hz sampled at {} Hz",
            device::AUX_WAVE_FREQUENCY,
            device::AUX_SAMPLE_RATE
        );
        if cli.no_drop {
            terminal_println!("  random sample drops disabled (--no-drop)");
        } else {
            terminal_println!(
                "  randomly dropping one sample from each sample clock about once per minute"
            );
        }
        terminal_println!(
            "  capture buffer: test.capture(-1) trigger, test.capture(-2) status, \
             test.capture(-3) metadata, {}-{} f32 samples, ~{:.1}s delay",
            device::CAPTURE_SAMPLES_MIN,
            device::CAPTURE_SAMPLES_MAX,
            device::CAPTURE_DELAY_NS as f64 / 1e9
        );
        match cli.upload {
            crate::UploadKind::TlChibi => {
                terminal_println!(
                    "  firmware upload: tl-chibi images (--upload signed for packages)"
                )
            }
            crate::UploadKind::Signed => {
                let text = |bytes: &[u8]| {
                    String::from_utf8_lossy(bytes)
                        .trim_end_matches('\0')
                        .to_string()
                };
                terminal_println!(
                    "  firmware upload: signed development packages, magic {:?} board {:?} revision {}, signature unchecked",
                    text(device::PACKAGE.magic.as_bytes()),
                    text(device::PACKAGE.board_id.as_bytes()),
                    device::PACKAGE.hw_rev.value()
                )
            }
        }
    }

    fn handle_keyboard(&mut self) -> io::Result<bool> {
        while event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                let now_ns = self.now_ns();
                let mut sent = Queue::default();
                match key.code {
                    KeyCode::Char('d') => {
                        self.tree.each(now_ns, &mut sent, &mut |device, _| {
                            device.drop_samples_now()
                        });
                    }
                    KeyCode::Char('r') => {
                        self.tree.each(now_ns, &mut sent, &mut |device, up| {
                            device.reboot(now_ns, up)
                        });
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(false);
                    }
                    _ => {}
                }
                self.send(sent)?;
            }
        }
        Ok(true)
    }

    fn receive_packets(&mut self) -> io::Result<()> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((size, addr)) => {
                    if !self.accept_packet_from(addr)? {
                        continue;
                    }
                    match PacketView::parse_prefix(&buf[..size]) {
                        Ok((_, len)) if len == size => {
                            let mut sent = Queue::default();
                            self.tree.route_down(&mut buf, self.now_ns(), &mut sent);
                            self.send(sent)?;
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
                // Windows reports a port unreachable reply to an earlier send,
                // as when the host closed its socket, on the next receive.
                Err(err) if err.kind() == io::ErrorKind::ConnectionReset => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Sends the host what the devices sent, or nothing while none is
    /// connected.
    fn send(&self, sent: Queue) -> io::Result<()> {
        let Some(client) = self.client else {
            return Ok(());
        };
        for packet in sent.0 {
            self.socket.send_to(&packet, client.addr)?;
        }
        Ok(())
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
                let now_ns = self.now_ns();
                let mut sent = Queue::default();
                self.tree.each(now_ns, &mut sent, &mut |device, up| {
                    device.connected(now_ns, up)
                });
                self.send(sent)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use twinleaf::proto::rpc::RpcError;
    use twinleaf::tio::packet;

    /// Returns the simulator `args` start on a free port of loopback, and a
    /// host socket connected to it.
    fn start_simulator(args: &[&str]) -> (Simulator, UdpSocket) {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0"].iter().chain(args));
        let simulator = Simulator::new(&cli).unwrap();
        simulator.socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let port = simulator.socket.local_addr().unwrap().port();
        let host = UdpSocket::bind(("127.0.0.1", 0)).unwrap();
        host.connect(("127.0.0.1", port)).unwrap();
        host.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        (simulator, host)
    }

    /// Sends `packet` from `host`, lets `simulator` take it, and returns
    /// everything the simulator sent back, with its route.
    fn exchange(
        simulator: &mut Simulator,
        host: &UdpSocket,
        packet: &packet::Packet,
    ) -> Vec<packet::Packet> {
        host.send(packet.as_bytes()).unwrap();
        // The datagram may reach the socket after send returns.
        simulator.socket.set_nonblocking(false).unwrap();
        simulator.socket.peek_from(&mut [0; Packet::MAX_SIZE]).unwrap();
        simulator.socket.set_nonblocking(true).unwrap();
        simulator.receive_packets().unwrap();
        received(host)
    }

    /// Everything waiting on `host`.
    fn received(host: &UdpSocket) -> Vec<packet::Packet> {
        let mut received = Vec::new();
        let mut buf = [0u8; 1024];
        while let Ok(size) = host.recv(&mut buf) {
            received.push(packet::Packet::from_slice_prefix(&buf[..size]).unwrap().0);
        }
        received
    }

    fn route(text: &str) -> DeviceRoute {
        text.parse().unwrap()
    }

    /// The reply or error the RPC `id` drew, at the route it came from.
    fn answer(packets: &[packet::Packet], id: u16) -> Option<(DeviceRoute, Result<Vec<u8>, u16>)> {
        packets.iter().find_map(|packet| match packet.payload() {
            packet::Payload::RpcReply(reply) if reply.req_id.value() == id => {
                Some((packet.route(), Ok(reply.value.to_vec())))
            }
            packet::Payload::RpcError(error) if error.req_id.value() == id => {
                Some((packet.route(), Err(error.code)))
            }
            _ => None,
        })
    }

    /// Calls `name` with `args` at `at`, and returns its answer.
    fn call(
        simulator: &mut Simulator,
        host: &UdpSocket,
        name: &str,
        args: &[u8],
        at: &str,
    ) -> Result<Vec<u8>, u16> {
        let request = packet::Packet::rpc_request(name, args, 9, route(at)).unwrap();
        let (from, answered) = answer(&exchange(simulator, host, &request), 9).unwrap();
        assert_eq!(from, route(at));
        answered
    }

    /// Each device of a layout tells a host that connects it is there, at its
    /// own route, and nothing speaks for a route the layout leaves out.
    #[test]
    fn a_layout_boots_a_device_at_each_route() {
        let (mut simulator, host) = start_simulator(&["--layout", "/1,/1/0,/3"]);

        let received = exchange(
            &mut simulator,
            &host,
            &packet::Packet::heartbeat(route("/1")),
        );

        let routes: BTreeSet<_> = received.iter().map(packet::Packet::route).collect();
        assert_eq!(
            routes,
            [route("/1"), route("/1/0"), route("/3")]
                .into_iter()
                .collect()
        );
    }

    /// Every device of a layout keeps sending heartbeats, each at its route.
    #[test]
    fn every_device_of_a_layout_beats_at_its_route() {
        let (mut simulator, host) = start_simulator(&["--layout", "/,/0,/0/2"]);
        exchange(
            &mut simulator,
            &host,
            &packet::Packet::heartbeat(route("/")),
        );

        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            let mut sent = Queue::default();
            simulator.tree.tick(simulator.now_ns(), &mut sent);
            simulator.send(sent).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }

        let beating: BTreeSet<_> = received(&host)
            .iter()
            .filter(|packet| matches!(packet.payload(), packet::Payload::Heartbeat(_)))
            .map(packet::Packet::route)
            .collect();
        assert_eq!(
            beating,
            [route("/"), route("/0"), route("/0/2")]
                .into_iter()
                .collect()
        );
    }

    /// An RPC is answered by the device at its route, through the hubs above
    /// it, and an RPC to a route with no device goes unanswered.
    #[test]
    fn an_rpc_is_answered_only_by_the_device_at_its_route() {
        let (mut simulator, host) = start_simulator(&["--layout", "/1,/1/0"]);
        exchange(
            &mut simulator,
            &host,
            &packet::Packet::heartbeat(route("/1")),
        );

        assert_eq!(
            call(&mut simulator, &host, "dev.name", &[], "/1/0"),
            Ok(b"tio-test".to_vec())
        );
        assert_eq!(
            call(&mut simulator, &host, "dev.serial", &[], "/1/0"),
            Ok(b"SIM0002".to_vec())
        );

        let request = packet::Packet::rpc_request("dev.name", &[], 8, DeviceRoute::root()).unwrap();
        assert_eq!(answer(&exchange(&mut simulator, &host, &request), 8), None);
        let request = packet::Packet::rpc_request("dev.name", &[], 8, route("/1/1")).unwrap();
        assert_eq!(answer(&exchange(&mut simulator, &host, &request), 8), None);
    }

    /// A setting takes a value its rule allows, and refuses one it does not.
    #[test]
    fn a_setting_refuses_a_value_out_of_range() {
        let (mut simulator, host) = start_simulator(&[]);

        assert_eq!(
            call(
                &mut simulator,
                &host,
                "test.amplitude",
                &2.5f64.to_le_bytes(),
                "/"
            ),
            Ok(2.5f64.to_le_bytes().to_vec())
        );
        assert_eq!(
            call(
                &mut simulator,
                &host,
                "test.amplitude",
                &(-1f64).to_le_bytes(),
                "/"
            ),
            Err(RpcError::Range.value())
        );
        assert_eq!(simulator.tree.devices()[0].board().amplitude(), 2.5);
    }

    /// A capture is busy while it is taken, then serves its blocks.
    #[test]
    fn a_capture_serves_its_blocks_once_taken() {
        let (mut simulator, host) = start_simulator(&[]);
        let selector = |value: i16| value.to_le_bytes();

        assert_eq!(
            call(&mut simulator, &host, "test.capture", &selector(-1), "/"),
            Ok(Vec::new())
        );
        assert_eq!(
            call(&mut simulator, &host, "test.capture", &selector(-2), "/"),
            Ok(vec![1])
        );
        assert_eq!(
            call(&mut simulator, &host, "test.capture", &selector(0), "/"),
            Err(RpcError::Busy.value())
        );

        std::thread::sleep(Duration::from_nanos(device::CAPTURE_DELAY_NS));
        assert_eq!(
            call(&mut simulator, &host, "test.capture", &selector(-2), "/"),
            Ok(vec![2])
        );
        let block = call(&mut simulator, &host, "test.capture", &selector(0), "/").unwrap();
        assert_eq!(block.len(), 256);
    }

    /// A dropped sample leaves one gap in its stream's sample numbers.
    #[test]
    fn a_dropped_sample_leaves_a_gap() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--no-drop"]);
        let mut device = SimDevice::new(&cli, DeviceRoute::root(), 0, 0);
        let mut sent = Queue::default();
        device.tick(10_000_000, &mut sent);
        device.drop_samples_now();
        device.tick(20_000_000, &mut sent);

        let numbers: Vec<u32> = sent
            .0
            .iter()
            .filter_map(|bytes| {
                let (packet, _) = packet::Packet::from_slice_prefix(bytes).ok()?;
                match packet.payload() {
                    packet::Payload::Samples(samples) if samples.stream_id.value() == 1 => {
                        let first = samples.first.value();
                        let count = (samples.data.len() / 16) as u32;
                        Some((first..first + count).collect::<Vec<_>>())
                    }
                    _ => None,
                }
            })
            .flatten()
            .collect();
        let expected: Vec<u32> = (0..20).filter(|&number| number != 10).collect();
        assert_eq!(numbers, expected);
    }

    /// `dev.stop` stops the samples, and `dev.start` begins them again.
    #[test]
    fn a_stopped_device_sends_no_samples() {
        let (mut simulator, host) = start_simulator(&[]);
        call(&mut simulator, &host, "dev.stop", &[], "/").unwrap();
        let mut sent = Queue::default();
        simulator
            .tree
            .tick(simulator.now_ns() + 50_000_000, &mut sent);
        let samples = |sent: &Queue| {
            sent.0
                .iter()
                .filter(|bytes| {
                    let (packet, _) = packet::Packet::from_slice_prefix(bytes).unwrap();
                    matches!(packet.payload(), packet::Payload::Samples(_))
                })
                .count()
        };
        assert_eq!(samples(&sent), 0);

        call(&mut simulator, &host, "dev.start", &[], "/").unwrap();
        let mut sent = Queue::default();
        simulator
            .tree
            .tick(simulator.now_ns() + 50_000_000, &mut sent);
        assert!(samples(&sent) > 0);
    }

    /// An upload counts the image bytes of each chunk as tl-chibi does, and an
    /// upgrade reboots into a new build.
    #[test]
    fn an_upgrade_reboots_into_a_new_build() {
        let (mut simulator, host) = start_simulator(&[]);
        let session = simulator.tree.devices()[0].session();

        call(&mut simulator, &host, "dev.firmware.upload", &[0; 288], "/").unwrap();
        assert_eq!(
            call(&mut simulator, &host, "dev.firmware.upload", &[], "/"),
            Ok(256u32.to_le_bytes().to_vec())
        );
        call(&mut simulator, &host, "dev.firmware.upgrade", &[], "/").unwrap();

        assert_ne!(simulator.tree.devices()[0].session(), session);
        let desc = call(&mut simulator, &host, "dev.desc", &[], "/").unwrap();
        assert!(String::from_utf8(desc)
            .unwrap()
            .ends_with("[2026-06-08/000002]"));
        assert_eq!(
            call(&mut simulator, &host, "dev.firmware.abort", &[], "/"),
            Err(RpcError::NotFound.value()),
            "tl-chibi has no abort"
        );
    }

    /// The header of a signed package for the simulator, around an image of
    /// `len` bytes.
    fn package_header(len: u32) -> Vec<u8> {
        let mut header = vec![0u8; 288];
        header[..8].copy_from_slice(device::PACKAGE.magic.as_bytes());
        header[8..10].copy_from_slice(&1u16.to_le_bytes());
        header[10..12].copy_from_slice(&288u16.to_le_bytes());
        header[12..20].copy_from_slice(device::PACKAGE.board_id.as_bytes());
        header[20..22].copy_from_slice(&device::PACKAGE.hw_rev.to_le_bytes());
        header[22..24].copy_from_slice(&1u16.to_le_bytes());
        header[24..28].copy_from_slice(&len.to_le_bytes());
        header
    }

    /// With `--upload signed`, the simulator takes a package as a device built
    /// on `twinleaf-device` does, and refuses a chunk out of place.
    #[test]
    fn a_signed_upload_takes_a_package_and_reboots_into_it() {
        let (mut simulator, host) = start_simulator(&["--upload", "signed"]);
        let session = simulator.tree.devices()[0].session();
        let upload = |simulator: &mut Simulator, args: &[u8]| {
            call(simulator, &host, "dev.firmware.upload", args, "/")
        };
        let chunk = |offset: u32, len: usize| {
            let mut chunk = offset.to_le_bytes().to_vec();
            chunk.resize(4 + len, 0xa5);
            chunk
        };

        assert_eq!(
            upload(&mut simulator, &[0; 288]),
            Err(RpcError::Invalid.value()),
            "a tl-chibi chunk is no package header"
        );
        upload(&mut simulator, &package_header(300)).unwrap();
        upload(&mut simulator, &chunk(0, 256)).unwrap();
        assert_eq!(
            upload(&mut simulator, &chunk(0, 44)),
            Err(RpcError::Range.value()),
            "a chunk already written"
        );
        upload(&mut simulator, &chunk(256, 44)).unwrap();
        assert_eq!(
            upload(&mut simulator, &[]),
            Ok(300u32.to_le_bytes().to_vec())
        );
        call(&mut simulator, &host, "dev.firmware.upgrade", &[], "/").unwrap();

        assert_ne!(simulator.tree.devices()[0].session(), session);
        assert_eq!(upload(&mut simulator, &[]), Ok(0u32.to_le_bytes().to_vec()));
    }

    /// The devices of a layout tell themselves apart by serial and session.
    #[test]
    fn the_devices_of_a_layout_have_their_own_serials_and_sessions() {
        let (simulator, _host) = start_simulator(&["--layout", "/,/0,/1"]);
        let devices = simulator.tree.devices();

        let serials: BTreeSet<_> = devices.iter().map(|device| device.serial()).collect();
        let sessions: BTreeSet<_> = devices.iter().map(|device| device.session()).collect();
        assert_eq!(serials.len(), 3);
        assert_eq!(sessions.len(), 3);
        assert_eq!(devices[0].serial(), device::DEVICE_SERIAL);
    }

    #[test]
    fn a_route_given_twice_is_refused() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0", "--layout", "/0,/1,/0"]);
        let refused = Simulator::new(&cli).map(drop).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::InvalidInput);
    }

    /// A device's hub reaches nothing through a port with no device on it, so
    /// a layout that routes through one is refused.
    #[test]
    fn a_route_through_a_devices_empty_port_is_refused() {
        let cli = SimulateCli::parse_from(["tio-simulate", "--port", "0", "--layout", "/,/2/0"]);
        let refused = Simulator::new(&cli).map(drop).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::InvalidInput);
    }

    /// A route whose parent the layout leaves out names that parent, the
    /// nearest one missing, and is reached through it.
    #[test]
    fn a_layout_names_the_parents_it_leaves_out() {
        let (mut simulator, host) = start_simulator(&["--layout", "/0/0,/0/1"]);
        assert_eq!(
            tree::missing_parent(&simulator.routes, &route("/0/0")),
            Some(route("/0"))
        );
        exchange(
            &mut simulator,
            &host,
            &packet::Packet::heartbeat(route("/0/0")),
        );
        assert_eq!(
            call(&mut simulator, &host, "dev.name", &[], "/0/1"),
            Ok(b"tio-test".to_vec())
        );

        let (simulator, _host) = start_simulator(&["--layout", "/0"]);
        assert_eq!(
            tree::missing_parent(&simulator.routes, &route("/0")),
            Some(DeviceRoute::root())
        );
    }
}

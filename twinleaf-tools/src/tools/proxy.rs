//! tio proxy
//!
//! Multiplexes access to one or more sensors, exposing the functionality of
//! tio::proxy via TCP. With `--mount`, each sensor hangs off a route prefix
//! and the proxy presents the set as a single virtual hub.

mod list;
mod nmea;

pub use list::run_list;

use crate::{MountArg, ProxyCli, ProxySubcommands};
#[cfg(feature = "mdns")]
use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use twinleaf::device::discovery::{self, DiscoveredDevice, PortInterface};
use twinleaf::device::runtime;
use twinleaf::proto;
use twinleaf::proto::log::LogLevel;
use twinleaf::tio::{self, packet, proxy};

/// Holders log their own lifecycle in full and never relay device logs.
fn init_proxy_logging(verbose: bool, debug: bool, holder: bool) {
    use std::io::Write;
    let level_filter = if holder {
        "info,twinleaf=debug,twinleaf_tools=debug,device=off"
    } else if debug {
        "trace"
    } else if verbose {
        "debug"
    } else {
        "info,device=debug"
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level_filter))
        .format(|buf, record| {
            let level = record.level();
            let level_style = buf.default_level_style(level);
            let target = record.target();
            let source = target
                .strip_prefix("device::")
                .unwrap_or_else(|| target.rsplit("::").next().unwrap_or(target));
            let bold = env_logger::fmt::style::Style::new().bold();
            let ts = chrono::Local::now().format("%T%.3f");
            writeln!(
                buf,
                "{ts} {level_style}{level:5}{level_style:#} {bold}{source}:{bold:#} {}",
                record.args()
            )
        })
        .init();
}

pub fn run_proxy(mut proxy_cli: ProxyCli) -> eyre::Result<()> {
    match proxy_cli.subcommands.take() {
        Some(ProxySubcommands::Stop { url }) => Ok(runtime::stop(&url)?),
        Some(ProxySubcommands::List(cli)) => list::run_list(cli),
        Some(ProxySubcommands::Nmea { tio, tcp_port }) => {
            init_proxy_logging(false, false, false);
            nmea::run_nmea_proxy(tio, tcp_port)
        }
        None => {
            if proxy_cli.detach {
                let endpoint = match proxy_cli.mounts.as_slice() {
                    [] => runtime::detach(proxy_cli.sensor_url.as_deref().unwrap_or("auto"))?,
                    mounts => runtime::compose(
                        &mounts
                            .iter()
                            .map(|m| (m.locator.clone(), m.prefix))
                            .collect::<Vec<_>>(),
                    )?,
                };
                println!("{endpoint}");
                return Ok(());
            }
            if proxy_cli.enumerate {
                return list::list_devices_deprecated(true);
            }
            let holder = match proxy_cli.holder_key.as_deref().map(runtime::Holder::claim) {
                None => None,
                Some(Ok(holder)) => Some(holder),
                Some(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Some(Err(e)) => return Err(e.into()),
            };
            let mounts = std::mem::take(&mut proxy_cli.mounts);
            let layout = Layout::from_cli(mounts, proxy_cli.sensor_url.take())?;

            init_proxy_logging(proxy_cli.verbose, proxy_cli.debug, holder.is_some());
            if proxy_cli.timestamp_format != "%T%.3f " {
                log::warn!(
                    "--timestamp is deprecated and no longer applied; \
                     timestamps are emitted by the logger"
                );
            }
            if proxy_cli.auto {
                log::warn!(
                    "'--auto' is deprecated; running without a URL now auto-detects by default"
                );
            }

            let server = ProxyServer {
                config: ProxyConfig::from(&proxy_cli),
                layout,
                holder,
                hosted: false,
            };
            server.run()
        }
    }
}

/// Make the devices picked in `tio proxy list` the default for every tool
/// until Ctrl-C: one device by staying connected to it, several by hosting a
/// hub of them.
pub fn run_proxy_for(picked: Vec<(DiscoveredDevice, proto::DeviceRoute)>) -> eyre::Result<()> {
    use clap::Parser;
    if let [(device, route)] = picked.as_slice() {
        if route.is_empty() {
            let _connection = twinleaf::Connection::open(&device.url)?;
            println!(
                "Using {} as the default while this runs. Ctrl-C to stop.",
                device.name.as_deref().unwrap_or(&device.url)
            );
            loop {
                std::thread::park();
            }
        }
    }
    let cli = ProxyCli::parse_from(["tio-proxy"]);
    init_proxy_logging(cli.verbose, cli.debug, false);
    let mounts: Vec<_> = picked
        .iter()
        .map(|(device, prefix)| (device.url.clone(), *prefix))
        .collect();
    let holder = match runtime::Holder::claim(&runtime::composition_key(&mounts)) {
        Ok(holder) => holder,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            eyre::bail!("the selection is already in use by another tio process")
        }
        Err(e) => return Err(e.into()),
    };
    ProxyServer {
        config: ProxyConfig::from(&cli),
        layout: Layout {
            mounts: picked
                .into_iter()
                .map(|(device, prefix)| Mount {
                    locator: device.url,
                    prefix,
                    auto_detected: false,
                    picked_name: device.name,
                })
                .collect(),
        },
        holder: Some(holder),
        hosted: true,
    }
    .run()
}

/// Server settings, fixed at startup.
#[derive(Debug, Clone)]
struct ProxyConfig {
    tcp_port: u16,
    // Only consulted by the mDNS advertising path.
    #[cfg_attr(not(feature = "mdns"), allow(dead_code))]
    mdns: bool,
    reconnect_timeout: Duration,
    disconnect_slow: bool,
    verbose: bool,
    debugging: bool,
    subtree: proto::DeviceRoute,
    dump_traffic: bool,
    dump_data: bool,
    dump_meta: bool,
    dump_hb: bool,
}

impl From<&ProxyCli> for ProxyConfig {
    fn from(cli: &ProxyCli) -> Self {
        Self {
            tcp_port: cli.port,
            mdns: cli.mdns,
            reconnect_timeout: Duration::from_secs(cli.reconnect_timeout),
            disconnect_slow: cli.kick_slow,
            verbose: cli.verbose,
            debugging: cli.debug,
            subtree: cli.subtree,
            dump_traffic: cli.dump,
            dump_data: cli.dump_data,
            dump_meta: cli.dump_meta,
            dump_hb: cli.dump_hb,
        }
    }
}

/// One upstream sensor bound to a route prefix. A single device without
/// `--mount` sits at the root prefix.
#[derive(Debug, Clone)]
struct Mount {
    locator: String,
    prefix: proto::DeviceRoute,
    auto_detected: bool,
    /// Device name when chosen through the `tio proxy list` picker.
    picked_name: Option<String>,
}

#[derive(Debug, Clone)]
struct Layout {
    mounts: Vec<Mount>,
}

impl Layout {
    fn from_cli(mount_args: Vec<MountArg>, sensor_url: Option<String>) -> eyre::Result<Layout> {
        if mount_args.is_empty() {
            return Ok(Layout {
                mounts: vec![resolve_root_mount(sensor_url)?],
            });
        }
        let mut prefixes = std::collections::HashSet::new();
        for arg in &mount_args {
            if !prefixes.insert(arg.prefix) {
                return Err(eyre::eyre!("duplicate mount prefix {}", arg.prefix));
            }
        }
        Ok(Layout {
            mounts: mount_args
                .into_iter()
                .map(|arg| Mount {
                    locator: arg.locator,
                    prefix: arg.prefix,
                    auto_detected: false,
                    picked_name: None,
                })
                .collect(),
        })
    }
}

/// Resolve the sensor URL into a root-prefix `Mount`. With no URL,
/// instantly auto-detects a single hard-wired serial device; network and
/// multi-device discovery live in `tio proxy list`.
fn resolve_root_mount(sensor_url: Option<String>) -> eyre::Result<Mount> {
    let (locator, auto_detected) = match sensor_url {
        Some(url) => (url, false),
        None => (auto_detect_serial()?, true),
    };

    Ok(Mount {
        locator,
        prefix: proto::DeviceRoute::root(),
        auto_detected,
        picked_name: None,
    })
}

/// Instant serial-only auto-detection for `tio proxy` with no URL. Errors
/// with the list when the choice is ambiguous.
fn auto_detect_serial() -> eyre::Result<String> {
    use color_eyre::Help;

    let mut valid_urls = Vec::new();
    for dev in discovery::enumerate_serial(false) {
        if matches!(dev.interface, PortInterface::STM32 | PortInterface::FTDI) {
            valid_urls.push(dev.url);
        }
    }
    if valid_urls.is_empty() {
        return Err(eyre::eyre!("no sensors detected").suggestion(
            "specify a URL, or run 'tio proxy list' to discover devices on the network",
        ));
    }
    if valid_urls.len() > 1 {
        eprintln!("multiple sensors detected:");
        let query_timeout = Duration::from_millis(500);
        for url in &valid_urls {
            match discovery::query_name(url, query_timeout) {
                Some(name) => eprintln!("  {}  {}", url, name),
                None => eprintln!("  {}  (no response)", url),
            }
        }
        return Err(eyre::eyre!("multiple sensors detected, cannot auto-select")
            .suggestion("pick one interactively with 'tio proxy list', or specify a URL")
            .suggestion("or mount each at a route prefix with --mount <url>=/N"));
    }
    Ok(valid_urls.swap_remove(0))
}

/// A mounted device's live connection: the proxy interface, its status
/// events, and the server's own monitoring port on it.
struct DeviceLink {
    prefix: proto::DeviceRoute,
    interface: proxy::Connection,
    status_rx: crossbeam::channel::Receiver<proxy::Event>,
    monitor_port: proxy::Port,
}

/// What a ready `Select` slot in the server loop corresponds to, recorded
/// at registration so readiness never has to be decoded from index math.
#[derive(Clone, Copy)]
enum Source<'a> {
    NewClient,
    Status(&'a DeviceLink),
    DevicePacket(&'a DeviceLink),
}

/// Why a client's forwarding loop ended.
enum Disconnect {
    ClientClosed,
    TooSlow,
    PortReceiveFailed,
    PortForwardFailed,
}

fn is_rpc(pkt: &packet::Packet) -> bool {
    matches!(
        pkt.ptype(),
        proto::PacketType::RPC_REQ | proto::PacketType::RPC_REP | proto::PacketType::RPC_ERROR
    )
}

/// Tracks a client that isn't keeping up, so the drop and the recovery are
/// each reported once rather than per packet.
#[derive(Default)]
struct SlowTracker {
    is_slow: bool,
    dropped: usize,
}

impl SlowTracker {
    fn packet_dropped(&mut self, addr: &str) {
        if !log::log_enabled!(log::Level::Debug) {
            return;
        }
        if !self.is_slow {
            self.is_slow = true;
            log::debug!("Client {} is not keeping up and is dropping packets", addr);
        }
        self.dropped += 1;
    }

    fn packet_delivered(&mut self, addr: &str) {
        if self.is_slow {
            log::debug!(
                "Client {} resuming after having dropped {} packets",
                addr,
                self.dropped
            );
            self.is_slow = false;
            self.dropped = 0;
        }
    }
}

/// The server fronting the TCP port: fans the mounted devices' traffic out
/// to TCP clients.
struct ProxyServer {
    config: ProxyConfig,
    layout: Layout,
    holder: Option<runtime::Holder>,
    /// A selection the user is hosting from the terminal: it never idles out.
    hosted: bool,
}

/// Counts a client thread until it exits.
struct Departure(Arc<AtomicUsize>);

impl Drop for Departure {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ProxyServer {
    fn run(self) -> eyre::Result<()> {
        use color_eyre::{Help, SectionExt};
        use eyre::bail;

        self.print_startup();

        let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);
        let endpoint = match &self.holder {
            Some(_) => create_listener_thread(SocketAddr::from(([127, 0, 0, 1], 0)), client_send)?,
            None => self.start_listeners(client_send)?,
        };
        let endpoint = format!("tcp://{endpoint}");

        // Phase 1: open each upstream interface. No monitor port is created yet,
        // so the sensor discovery in phase 2 runs without an undrained port: the
        // proxy thread blocks delivering to a full monitor port, and a blocked
        // proxy thread can't service the client registration that `new_port`
        // (used by discovery) waits on — which would deadlock advertising.
        struct PendingLink {
            prefix: proto::DeviceRoute,
            locator: String,
            interface: proxy::Connection,
            status_rx: crossbeam::channel::Receiver<proxy::Event>,
        }
        let mut pending = Vec::with_capacity(self.layout.mounts.len());
        for mount in &self.layout.mounts {
            let locator = match (&self.holder, self.hosted) {
                (Some(_), false) => mount.locator.clone(),
                (Some(_), true) | (None, _) => runtime::resolve(&mount.locator)?,
            };
            let (status_send, status_rx) = crossbeam::channel::bounded::<proxy::Event>(100);
            let interface = proxy::Connection::open_with(
                &locator,
                Some(self.config.reconnect_timeout),
                Some(status_send),
            );
            pending.push(PendingLink {
                prefix: mount.prefix,
                locator: mount.locator.clone(),
                interface,
                status_rx,
            });
        }

        // Phase 2: advertise this proxy over mDNS so other hosts find it via
        // `tio proxy list`, naming it after the sensors discovered on the bare
        // interfaces (discovery's own ports are drained and dropped, so nothing
        // backs up). The guard lives until `run` returns, then sends goodbyes.
        #[cfg(feature = "mdns")]
        let _mdns = {
            let interfaces: Vec<&proxy::Connection> =
                pending.iter().map(|p| &p.interface).collect();
            self.advertise_mdns(&interfaces)
        };

        // Phase 3: open the proxy's own monitor port on each interface (used to
        // receive log messages and dump traffic) and build the links. The select
        // loop below drains these immediately, so they never back up.
        let mut links = Vec::with_capacity(pending.len());
        for p in pending {
            let monitor_port = match proxy::open_port(
                &p.interface,
                None,
                self.config.subtree,
                usize::MAX,
                true,
                true,
            ) {
                Ok(port) => port,
                Err(e) => {
                    let last_status = p.status_rx.iter().last();
                    let err = eyre::Report::new(e)
                        .wrap_err(format!("could not open port on {}", p.locator));
                    return Err(if let Some(status) = last_status {
                        err.with_section(move || {
                            format!("{:?}", status).header("Last proxy event:")
                        })
                    } else {
                        err
                    });
                }
            };
            links.push(DeviceLink {
                prefix: p.prefix,
                interface: p.interface,
                status_rx: p.status_rx,
                monitor_port,
            });
        }

        if let Some(holder) = &self.holder {
            for link in &links {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match link.status_rx.recv_deadline(deadline) {
                        Ok(proxy::Event::SensorConnected | proxy::Event::SensorReconnected) => {
                            break
                        }
                        Ok(
                            proxy::Event::FailedToConnect
                            | proxy::Event::FailedToReconnect
                            | proxy::Event::Exiting,
                        ) => bail!("holder upstream {} failed to connect", link.prefix),
                        Ok(event) => log_proxy_event(event, &link.prefix),
                        Err(_) => {
                            bail!("holder upstream {} sent no packet within 10s", link.prefix)
                        }
                    }
                }
            }
            holder.publish(&endpoint)?;
            if self.hosted {
                println!("Serving the selection as the default for every tool. Ctrl-C to stop.");
            } else {
                log::info!(
                    "holder ready at {endpoint} ({})",
                    std::env::current_exe().unwrap_or_default().display()
                );
            }
        }

        let mut sel = crossbeam::channel::Select::new();
        let mut sources = Vec::with_capacity(1 + 2 * links.len());
        sel.recv(&new_client);
        sources.push(Source::NewClient);
        for link in &links {
            sel.recv(&link.status_rx);
            sources.push(Source::Status(link));
            sel.recv(link.monitor_port.receiver());
            sources.push(Source::DevicePacket(link));
        }

        let clients = Arc::new(AtomicUsize::new(0));
        let mut idle_since = Instant::now();
        let mut next_tick = Instant::now();
        loop {
            if let Some(holder) = self.holder.as_ref().filter(|_| Instant::now() >= next_tick) {
                next_tick = Instant::now() + Duration::from_millis(250);
                if clients.load(Ordering::Relaxed) > 0 {
                    idle_since = Instant::now();
                }
                if !holder.published() {
                    log::info!("stopped by tio proxy stop");
                    break;
                }
                if idle_since.elapsed() >= runtime::IDLE && !holder.pinned() && !self.hosted {
                    holder.withdraw()?;
                    std::thread::sleep(runtime::WITHDRAWAL);
                    if clients.load(Ordering::Relaxed) == 0 && new_client.is_empty() {
                        log::info!("no clients for {:?}; exiting", runtime::IDLE);
                        break;
                    }
                    holder.publish(&endpoint)?;
                }
            }
            let oper = match &self.holder {
                None => sel.select(),
                Some(_) => match sel.select_deadline(next_tick) {
                    Ok(oper) => oper,
                    Err(_) => continue,
                },
            };
            match sources[oper.index()] {
                Source::NewClient => {
                    let Ok(stream) = oper.recv(&new_client) else {
                        bail!("listener thread died unexpectedly");
                    };
                    self.accept_client(stream, &links, &clients);
                }
                Source::Status(link) => {
                    let Ok(evt) = oper.recv(&link.status_rx) else {
                        // The proxy thread died, most likely due to the sensor
                        // getting disconnected past the autoreconnection
                        break;
                    };
                    log_proxy_event(evt, &link.prefix);
                }
                Source::DevicePacket(link) => {
                    let Ok(pkt) = oper.recv(link.monitor_port.receiver()) else {
                        break;
                    };
                    self.log_device_packet(pkt, &link.prefix);
                }
            }
        }
        Ok(())
    }

    /// Advertise this proxy over mDNS (only when opted in). Returns a guard
    /// that keeps the advertisement live until dropped.
    #[cfg(feature = "mdns")]
    fn advertise_mdns(&self, interfaces: &[&proxy::Connection]) -> Option<MdnsService> {
        if !self.config.mdns {
            return None;
        }
        let host = gethostname::gethostname().to_string_lossy().into_owned();
        let instance = self.mdns_instance_name(interfaces, &host);
        match advertise_tcp(&instance, &host, self.config.tcp_port) {
            Some(service) => {
                log::info!("advertising over mDNS as \"{instance}\"");
                Some(service)
            }
            None => {
                log::warn!("could not advertise over mDNS");
                None
            }
        }
    }

    /// Name shown to other hosts in `tio proxy list`, built from the sensors actually
    /// connected: discover every route on each upstream link, count the sensor
    /// models (ignoring COMM/HUB routing devices), and render e.g. `VMR (x2)`.
    /// Falls back to `tio-proxy (host)` when no sensors are found.
    #[cfg(feature = "mdns")]
    fn mdns_instance_name(&self, interfaces: &[&proxy::Connection], host: &str) -> String {
        let mut models = Vec::new();
        for &interface in interfaces {
            models.extend(discover_sensor_models(interface));
        }

        if let Some(summary) = summarize_sensors(&models) {
            return summary;
        }

        let short = host.trim_end_matches('.').trim_end_matches(".local");
        format!("tio-proxy ({short})")
    }

    fn print_startup(&self) {
        let mounts = &self.layout.mounts;
        match mounts.as_slice() {
            [single] => match single.picked_name.as_deref() {
                Some(name) => println!("tio proxy starting (selected {name}):"),
                None => println!("tio proxy starting:"),
            },
            _ => println!("tio proxy starting:"),
        }
        if mounts.len() == 1 && mounts[0].prefix.is_empty() {
            println!(
                "  Sensor: {}{}",
                mounts[0].locator,
                if mounts[0].auto_detected {
                    " (auto-detected)"
                } else {
                    ""
                }
            );
        } else {
            println!("  Mounts:");
            for mount in mounts {
                match mount.picked_name.as_deref() {
                    Some(name) => println!("    {}  {}  ({})", mount.prefix, mount.locator, name),
                    None => println!("    {}  {}", mount.prefix, mount.locator),
                }
            }
        }
        if self.holder.is_none() {
            println!("  TCP port: {}", self.config.tcp_port);
        }
        println!("  Subtree: {}", self.config.subtree);

        let flags = [
            ("verbose", self.config.verbose),
            ("debug", self.config.debugging),
            ("kick-slow", self.config.disconnect_slow),
            ("dump", self.config.dump_traffic),
            ("dump-data", self.config.dump_data),
            ("dump-meta", self.config.dump_meta),
            ("dump-hb", self.config.dump_hb),
        ];
        let enabled: Vec<&str> = flags
            .iter()
            .filter_map(|&(name, on)| on.then_some(name))
            .collect();
        if !enabled.is_empty() {
            println!("  Flags: {}", enabled.join(" "));
        }
        println!();
    }

    fn start_listeners(
        &self,
        client_send: crossbeam::channel::Sender<std::net::TcpStream>,
    ) -> eyre::Result<SocketAddr> {
        use color_eyre::Help;

        let started_v6 = create_listener_thread(
            std::net::SocketAddr::new(
                std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                self.config.tcp_port,
            ),
            client_send.clone(),
        );
        // Outside windows a v6 socket also accepts v4 clients.
        if let (Ok(addr), false) = (&started_v6, cfg!(windows)) {
            return Ok(*addr);
        }
        let started_v4 = create_listener_thread(
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                self.config.tcp_port,
            ),
            client_send,
        );
        match (started_v6, started_v4) {
            (Ok(addr), Ok(_)) | (Ok(addr), Err(_)) | (Err(_), Ok(addr)) => Ok(addr),
            (Err(e1), Err(e2)) => {
                let addr_in_use = matches!(e1.kind(), io::ErrorKind::AddrInUse)
                    || matches!(e2.kind(), io::ErrorKind::AddrInUse);
                let err = eyre::eyre!(
                    "could not bind TCP port {}: v6={}, v4={}",
                    self.config.tcp_port,
                    e1,
                    e2
                );
                Err(if addr_in_use {
                    err.suggestion(format!(
                        "another 'tio proxy' is likely running on port {}; try --port <N>",
                        self.config.tcp_port
                    ))
                } else {
                    err
                })
            }
        }
    }

    fn accept_client(
        &self,
        stream: std::net::TcpStream,
        links: &[DeviceLink],
        clients: &Arc<AtomicUsize>,
    ) {
        let addr = match stream.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(err) => {
                log::warn!("Failed to determine client address: {:?}", err);
                return;
            }
        };
        // A client from the proxy perspective is a port in reverse, i.e. what it receives
        // is what a client transmits, and vice-versa. Therefore, the channel size settings
        // for rx and tx are inverted. Also, we use the proxy port channel size setting
        // instead of the physical ports setting.
        let (rx_send, client_rx) =
            tio::transport::Port::rx_channel_custom(proxy::client_tx_channel_size());
        let client = match tio::transport::Port::from_tcp_stream_custom(
            stream,
            tio::transport::Port::rx_to_channel(rx_send),
            proxy::client_rx_channel_size(),
        ) {
            Ok(client_port) => client_port,
            _ => return,
        };

        log::debug!("Accepted client from {}", addr);
        let mut ports = Vec::with_capacity(links.len());
        for link in links {
            let port = proxy::open_port(
                &link.interface,
                Some(Duration::from_millis(2000)),
                self.config.subtree,
                usize::MAX,
                true,
                true,
            )
            .expect("Failed to create new proxy port");
            ports.push((link.prefix, port));
        }

        let dump_traffic = self.config.dump_traffic;
        let disconnect_slow = self.config.disconnect_slow;
        clients.fetch_add(1, Ordering::Relaxed);
        let departure = Departure(clients.clone());
        std::thread::spawn(move || {
            let _departure = departure;
            let mut slow = SlowTracker::default();

            // Slot 0 is the client's own traffic; slot 1 + i is ports[i].
            let mut sel = crossbeam::channel::Select::new();
            sel.recv(&client_rx);
            for (_, port) in &ports {
                sel.recv(port.receiver());
            }

            let reason = loop {
                let oper = sel.select();
                match oper.index() {
                    0 => {
                        let Ok(Ok(pkt)) = oper.recv(&client_rx) else {
                            break Disconnect::ClientClosed;
                        };
                        if dump_traffic {
                            log::info!("{}->{} -- {:?}", addr, pkt.route(), pkt.payload());
                        }
                        let mut dest = None;
                        for (prefix, port) in &ports {
                            if let Ok(relative) = prefix.relative_route(&pkt.route()) {
                                dest = Some((relative, port));
                                break;
                            }
                        }
                        let Some((relative, port)) = dest else {
                            log::debug!(
                                "Client {} addressed unmounted route {}",
                                addr,
                                pkt.route()
                            );
                            continue;
                        };
                        if port.try_send(pkt.with_route(relative)).is_err() {
                            break Disconnect::PortForwardFailed;
                        }
                    }
                    i => {
                        let (prefix, port) = &ports[i - 1];
                        let Ok(pkt) = oper.recv(port.receiver()) else {
                            break Disconnect::PortReceiveFailed;
                        };
                        let Ok(routing) = prefix.absolute_route(&pkt.route()) else {
                            log::warn!(
                                "Dropping packet for client {}: route {} exceeds max depth",
                                addr,
                                pkt.route()
                            );
                            continue;
                        };
                        let pkt = pkt.with_route(routing);
                        if dump_traffic && is_rpc(&pkt) {
                            log::info!("{}->{} -- {:?}", pkt.route(), addr, pkt.payload());
                        }
                        match client.try_send(pkt) {
                            Ok(()) => slow.packet_delivered(&addr),
                            Err(tio::transport::SendError::Full) if !disconnect_slow => {
                                slow.packet_dropped(&addr)
                            }
                            Err(tio::transport::SendError::Full) => break Disconnect::TooSlow,
                            Err(_) => break Disconnect::ClientClosed,
                        }
                    }
                }
            };

            match reason {
                Disconnect::ClientClosed => log::debug!("Client {} exiting", addr),
                Disconnect::TooSlow => {
                    log::warn!("Disconnecting client {} due to slowness", addr)
                }
                Disconnect::PortReceiveFailed => log::warn!(
                    "Disconnecting client {} due to internal error receiving tio data in thread",
                    addr
                ),
                Disconnect::PortForwardFailed => log::warn!(
                    "Disconnecting client {} due to internal error forwarding tio data in thread",
                    addr
                ),
            }
        });
    }

    fn log_device_packet(&self, pkt: packet::Packet, prefix: &proto::DeviceRoute) {
        let Ok(routing) = prefix.absolute_route(&pkt.route()) else {
            log::warn!("Dropping packet whose mounted route exceeds max depth");
            return;
        };
        let pkt = pkt.with_route(routing);
        let payload = pkt.payload();
        let dump = match payload {
            packet::Payload::Heartbeat(_) => self.config.dump_hb,
            packet::Payload::Metadata(..) => self.config.dump_meta,
            packet::Payload::Samples(_) => self.config.dump_data,
            _ => self.config.dump_traffic,
        };
        if dump {
            log::info!("Packet from {} -- {:?}", routing, payload);
        }
        if let packet::Payload::Log(message) = payload {
            // Map the device-reported level onto the log crate's level
            // so the logger filter and prefix reflect it.
            let level = match message.level {
                LogLevel::CRITICAL | LogLevel::ERROR => log::Level::Error,
                LogLevel::WARNING => log::Level::Warn,
                LogLevel::DEBUG => log::Level::Debug,
                _ => log::Level::Info,
            };
            let text = String::from_utf8_lossy(message.message);
            log::log!(target: &format!("device::{routing}"), level, "{text}");
        }
    }
}

/// How long to listen for heartbeats to discover the connected sensors.
#[cfg(feature = "mdns")]
const MDNS_ROUTE_WINDOW: Duration = Duration::from_millis(500);

/// A live mDNS advertisement. Dropping it sends goodbye packets and stops the
/// responder, so other hosts stop discovering this proxy promptly.
#[cfg(feature = "mdns")]
pub struct MdnsService {
    daemon: mdns_sd::ServiceDaemon,
    fullname: String,
}

#[cfg(feature = "mdns")]
impl Drop for MdnsService {
    fn drop(&mut self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

/// Advertise a `_twinleaf._tcp` service on the local network so other hosts
/// discover this proxy (e.g. via `tio proxy list`).
///
/// `instance` is the human-readable name shown to browsers, `host_name` is the
/// machine's name (a `.local.` suffix is added if missing), and `port` is the
/// TCP port being served. The host's addresses are filled in automatically.
/// Keep the returned guard alive for as long as the service should be
/// advertised. Returns `None` if the responder can't be set up.
#[cfg(feature = "mdns")]
fn advertise_tcp(instance: &str, host_name: &str, port: u16) -> Option<MdnsService> {
    use mdns_sd::{ServiceDaemon, ServiceInfo};
    use std::collections::HashMap;

    let host = {
        let base = host_name.trim_end_matches('.').trim_end_matches(".local");
        format!("{base}.local.")
    };

    let daemon = ServiceDaemon::new().ok()?;
    let info = ServiceInfo::new(
        "_twinleaf._tcp.local.",
        instance,
        &host,
        (),
        port,
        None::<HashMap<String, String>>,
    )
    .ok()?
    // Publish the host's current addresses (and track interface changes).
    .enable_addr_auto();

    let fullname = info.get_fullname().to_string();
    daemon.register(info).ok()?;
    Some(MdnsService { daemon, fullname })
}

/// Discover the device routes on `interface` and return the `dev.name` of
/// each, over the link the server already owns.
#[cfg(feature = "mdns")]
fn discover_sensor_models(interface: &proxy::Connection) -> Vec<String> {
    twinleaf::Connection::over(interface)
        .tree(proto::DeviceRoute::root())
        .named_routes(MDNS_ROUTE_WINDOW)
        .into_iter()
        .filter_map(|nr| nr.name)
        .collect()
}

/// Summarize discovered device names as a sensor count for the mDNS instance
/// name, ignoring COMM/HUB routing devices. e.g.
/// `["HUB-USB-TDS", "COMM-RS422", "VMR", "VMR"]` -> `"VMR (x2)"`. Multiple
/// models are joined with `", "`. Returns `None` if no sensors remain.
#[cfg(feature = "mdns")]
fn summarize_sensors(models: &[String]) -> Option<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for model in models {
        if !is_infrastructure(model) {
            *counts.entry(model.as_str()).or_default() += 1;
        }
    }
    if counts.is_empty() {
        return None;
    }
    Some(
        counts
            .iter()
            .map(|(model, &n)| {
                if n > 1 {
                    format!("{model} (x{n})")
                } else {
                    (*model).to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// COMM-* and HUB-* devices are routing/infrastructure, not sensors.
#[cfg(feature = "mdns")]
fn is_infrastructure(name: &str) -> bool {
    let upper = name.to_uppercase();
    upper.starts_with("COMM") || upper.starts_with("HUB")
}

#[cfg(all(test, feature = "mdns"))]
mod mdns_tests {
    use super::{is_infrastructure, summarize_sensors};

    fn models(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn excludes_comm_and_hub_routes() {
        assert!(is_infrastructure("COMM-RS422"));
        assert!(is_infrastructure("COMM-USB-RS422"));
        assert!(is_infrastructure("HUB-USB-TDS"));
        assert!(!is_infrastructure("VMR"));
        assert!(!is_infrastructure("AXIS"));
    }

    #[test]
    fn counts_duplicate_sensors() {
        // A hub with two VMRs -> "VMR (x2)", COMM/HUB left out.
        let got = summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422", "VMR", "VMR"]));
        assert_eq!(got.as_deref(), Some("VMR (x2)"));
    }

    #[test]
    fn single_sensor_has_no_count_suffix() {
        let got = summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422", "VMR"]));
        assert_eq!(got.as_deref(), Some("VMR"));
    }

    #[test]
    fn multiple_models_are_joined_sorted() {
        let got = summarize_sensors(&models(&["VMR", "VMR", "AXIS", "COMM-RS422"]));
        assert_eq!(got.as_deref(), Some("AXIS, VMR (x2)"));
    }

    #[test]
    fn no_sensors_yields_none() {
        assert_eq!(
            summarize_sensors(&models(&["HUB-USB-TDS", "COMM-RS422"])),
            None
        );
        assert_eq!(summarize_sensors(&[]), None);
    }
}

fn create_listener_thread(
    addr: std::net::SocketAddr,
    client_send: crossbeam::channel::Sender<std::net::TcpStream>,
) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    std::thread::Builder::new()
        .name("listener".to_string())
        .spawn(move || {
            for res in listener.incoming() {
                match res {
                    Ok(stream) => client_send.send(stream).expect("New client queue full"),
                    Err(err) => eprintln!("error accepting client: {}", err),
                };
            }
        })?;
    Ok(bound)
}

fn log_proxy_event(evt: proxy::Event, prefix: &proto::DeviceRoute) {
    let target = format!("proxy::{}", prefix);
    let target = target.as_str();
    match evt {
        proxy::Event::SensorDisconnected => {
            log::warn!(target: target, "Sensor disconnected");
        }
        proxy::Event::SensorReconnected => {
            log::info!(target: target, "Sensor reconnected");
        }
        proxy::Event::FailedToReconnect => {
            log::error!(target: target, "Stopping reconnection attempts due to timeout");
        }
        proxy::Event::FailedToConnect => {
            log::error!(target: target, "Fatal proxy error: failed to connect to sensor");
        }
        proxy::Event::FatalError(err) => {
            log::error!(target: target, "Fatal proxy error: {:?}", err);
            // the proxy thread will exit and we'll detect it at the next iteration.
        }
        proxy::Event::Text(txt) => {
            log::info!(target: target, "Text: {}", txt);
        }
        proxy::Event::ProtocolError(perr) => {
            log::debug!(target: target, "Protocol error: {:?}", perr);
        }
        evt => {
            log::trace!(target: target, "{:?}", evt);
        }
    }
}

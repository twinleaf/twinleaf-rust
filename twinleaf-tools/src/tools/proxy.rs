//! tio proxy
//!
//! Multiplexes access to one or more sensors, exposing the functionality of
//! tio::proxy via TCP. With `--mount`, each sensor hangs off a route prefix
//! and the proxy presents the set as a single virtual hub.

pub mod list;
mod nmea;

use crate::{MountArg, ProxyCli, ProxySubcommands};

pub use list::run_list;

use std::collections::BTreeMap;
use std::io::{self, IsTerminal};
use std::net::TcpListener;
use std::time::Duration;
use twinleaf::device::discovery::{self, PortInterface};
use twinleaf::device::DeviceTree;
use twinleaf::tio::{self, proto, proxy};

fn init_proxy_logging(verbose: bool, debug: bool) {
    use std::io::Write;
    let level_filter = if debug {
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
        Some(ProxySubcommands::Nmea { tio, tcp_port }) => {
            init_proxy_logging(false, false);
            nmea::run_nmea_proxy(tio, tcp_port)
        }
        None => {
            init_proxy_logging(proxy_cli.verbose, proxy_cli.debug);
            if proxy_cli.timestamp_format != "%T%.3f " {
                log::warn!(
                    "--timestamp is deprecated and no longer applied; \
                     timestamps are emitted by the logger"
                );
            }
            if proxy_cli.enumerate {
                return list::list_devices_deprecated(true);
            }
            if proxy_cli.auto {
                log::warn!(
                    "'--auto' is deprecated; running without a URL now auto-detects by default"
                );
            }
            let mounts = std::mem::take(&mut proxy_cli.mounts);
            let layout = Layout::from_cli(
                mounts,
                proxy_cli.sensor_url.take(),
                proxy_cli.discover_duration,
            )?;
            let server = ProxyServer {
                config: ProxyConfig::from(&proxy_cli),
                layout,
            };
            server.run()
        }
    }
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
}

#[derive(Debug, Clone)]
struct Layout {
    mounts: Vec<Mount>,
}

impl Layout {
    fn from_cli(
        mount_args: Vec<MountArg>,
        sensor_url: Option<String>,
        discover_duration: Duration,
    ) -> eyre::Result<Layout> {
        if mount_args.is_empty() {
            return Ok(Layout {
                mounts: vec![resolve_root_mount(sensor_url, discover_duration)?],
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
                })
                .collect(),
        })
    }
}

/// Resolve the sensor URL into a root-prefix `Mount`. With no URL, opens the
/// interactive device picker on a terminal, or falls back to serial-only
/// auto-detection when output isn't a TTY.
fn resolve_root_mount(
    sensor_url: Option<String>,
    discover_duration: Duration,
) -> eyre::Result<Mount> {
    let auto_detected = sensor_url.is_none();
    let locator = match sensor_url {
        Some(url) => url,
        None if std::io::stdout().is_terminal() => {
            let config = discovery::DiscoveryConfig {
                include_unknown: false,
                network: true,
                probe_names: true,
            };
            match crate::tui::selector::pick_device(config, discover_duration)? {
                Some(device) => device.url,
                None => return Err(eyre::eyre!("no device selected")),
            }
        }
        None => auto_detect_serial()?,
    };

    Ok(Mount {
        locator,
        prefix: proto::DeviceRoute::root(),
        auto_detected,
    })
}

/// Serial-only auto-detection used when output isn't a TTY (so the interactive
/// picker can't run). Errors with the list when the choice is ambiguous.
fn auto_detect_serial() -> eyre::Result<String> {
    use color_eyre::Help;

    let mut valid_urls = Vec::new();
    for dev in discovery::enumerate_serial(false) {
        if matches!(dev.interface, PortInterface::STM32 | PortInterface::FTDI) {
            valid_urls.push(dev.url);
        }
    }
    if valid_urls.is_empty() {
        return Err(eyre::eyre!("no sensors detected")
            .suggestion("specify a URL with -s <url>, or run 'tio list'"));
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
            .suggestion("specify one with -s <url>")
            .suggestion("or mount each at a route prefix with --mount <url>=/N"));
    }
    Ok(valid_urls.swap_remove(0))
}

/// A mounted device's live connection: the proxy interface, its status
/// events, and the server's own monitoring port on it.
struct DeviceLink {
    prefix: proto::DeviceRoute,
    interface: proxy::Interface,
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

fn is_rpc(payload: &proto::Payload) -> bool {
    matches!(
        payload,
        proto::Payload::RpcRequest(_) | proto::Payload::RpcReply(_) | proto::Payload::RpcError(_)
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
}

impl ProxyServer {
    fn run(self) -> eyre::Result<()> {
        use color_eyre::{Help, SectionExt};
        use eyre::bail;

        self.print_startup();

        let new_client = self.start_listeners()?;

        // Phase 1: open each upstream interface. No monitor port is created yet,
        // so the sensor discovery in phase 2 runs without an undrained port: the
        // proxy thread blocks delivering to a full monitor port, and a blocked
        // proxy thread can't service the client registration that `new_port`
        // (used by discovery) waits on — which would deadlock advertising.
        struct PendingLink {
            prefix: proto::DeviceRoute,
            locator: String,
            interface: proxy::Interface,
            status_rx: crossbeam::channel::Receiver<proxy::Event>,
        }
        let mut pending = Vec::with_capacity(self.layout.mounts.len());
        for mount in &self.layout.mounts {
            let (status_send, status_rx) = crossbeam::channel::bounded::<proxy::Event>(100);
            let interface = proxy::Interface::new_proxy(
                &mount.locator,
                Some(self.config.reconnect_timeout),
                Some(status_send),
            );
            pending.push(PendingLink {
                prefix: mount.prefix.clone(),
                locator: mount.locator.clone(),
                interface,
                status_rx,
            });
        }

        // Phase 2: advertise this proxy over mDNS so other hosts find it via
        // `tio list`, naming it after the sensors discovered on the bare
        // interfaces (discovery's own ports are drained and dropped, so nothing
        // backs up). The guard lives until `run` returns, then sends goodbyes.
        #[cfg(feature = "mdns")]
        let _mdns = {
            let interfaces: Vec<&proxy::Interface> = pending.iter().map(|p| &p.interface).collect();
            self.advertise_mdns(&interfaces)
        };

        // Phase 3: open the proxy's own monitor port on each interface (used to
        // receive log messages and dump traffic) and build the links. The select
        // loop below drains these immediately, so they never back up.
        let mut links = Vec::with_capacity(pending.len());
        for p in pending {
            let monitor_port = match p.interface.subtree_full(self.config.subtree.clone()) {
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

        loop {
            let oper = sel.select();
            match sources[oper.index()] {
                Source::NewClient => {
                    let Ok(stream) = oper.recv(&new_client) else {
                        bail!("listener thread died unexpectedly");
                    };
                    self.accept_client(stream, &links);
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
    fn advertise_mdns(&self, interfaces: &[&proxy::Interface]) -> Option<MdnsService> {
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

    /// Name shown to other hosts in `tio list`, built from the sensors actually
    /// connected: discover every route on each upstream link, count the sensor
    /// models (ignoring COMM/HUB routing devices), and render e.g. `VMR (x2)`.
    /// Falls back to `tio-proxy (host)` when no sensors are found.
    #[cfg(feature = "mdns")]
    fn mdns_instance_name(&self, interfaces: &[&proxy::Interface], host: &str) -> String {
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
        println!("tio proxy starting:");
        let mounts = &self.layout.mounts;
        if mounts.len() == 1 && mounts[0].prefix.len() == 0 {
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
                println!("    {}  {}", mount.prefix, mount.locator);
            }
        }
        println!("  TCP port: {}", self.config.tcp_port);
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

    fn start_listeners(&self) -> eyre::Result<crossbeam::channel::Receiver<std::net::TcpStream>> {
        use color_eyre::Help;

        let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);
        let started_v6 = create_listener_thread(
            std::net::SocketAddr::new(
                std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                self.config.tcp_port,
            ),
            client_send.clone(),
        );
        let started_v4 = if let (Ok(()), false) = (&started_v6, cfg!(windows)) {
            // If v6 started correctly and we are not in windows, pretend
            // v4 also started correctly. The OS will pass the new clients
            // through the v6 socket.
            Ok(())
        } else {
            create_listener_thread(
                std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    self.config.tcp_port,
                ),
                client_send.clone(),
            )
        };
        if let (Err(e1), Err(e2)) = (started_v6, started_v4) {
            let addr_in_use = matches!(e1.kind(), io::ErrorKind::AddrInUse)
                || matches!(e2.kind(), io::ErrorKind::AddrInUse);
            let err = eyre::eyre!(
                "could not bind TCP port {}: v6={}, v4={}",
                self.config.tcp_port,
                e1,
                e2
            );
            return Err(if addr_in_use {
                err.suggestion(format!(
                    "another 'tio proxy' is likely running on port {}; try --port <N>",
                    self.config.tcp_port
                ))
            } else {
                err
            });
        }
        Ok(new_client)
    }

    fn accept_client(&self, stream: std::net::TcpStream, links: &[DeviceLink]) {
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
            tio::transport::Port::rx_channel_custom(proxy::Interface::get_client_tx_channel_size());
        let client = match tio::transport::Port::from_tcp_stream_custom(
            stream,
            tio::transport::Port::rx_to_channel(rx_send),
            proxy::Interface::get_client_rx_channel_size(),
        ) {
            Ok(client_port) => client_port,
            _ => return,
        };

        log::debug!("Accepted client from {}", addr);
        let mut ports = Vec::with_capacity(links.len());
        for link in links {
            let port = link
                .interface
                .new_port(
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
        std::thread::spawn(move || {
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
                        let Ok(Ok(mut pkt)) = oper.recv(&client_rx) else {
                            break Disconnect::ClientClosed;
                        };
                        if dump_traffic {
                            log::info!("{}->{} -- {:?}", addr, pkt.routing, pkt.payload);
                        }
                        let mut dest = None;
                        for (prefix, port) in &ports {
                            if let Ok(relative) = prefix.relative_route(&pkt.routing) {
                                dest = Some((relative, port));
                                break;
                            }
                        }
                        let Some((relative, port)) = dest else {
                            log::debug!(
                                "Client {} addressed unmounted route {}",
                                addr,
                                pkt.routing
                            );
                            continue;
                        };
                        pkt.routing = relative;
                        if port.try_send(pkt).is_err() {
                            break Disconnect::PortForwardFailed;
                        }
                    }
                    i => {
                        let (prefix, port) = &ports[i - 1];
                        let Ok(mut pkt) = oper.recv(port.receiver()) else {
                            break Disconnect::PortReceiveFailed;
                        };
                        let Ok(routing) = prefix.absolute_route(&pkt.routing) else {
                            log::warn!(
                                "Dropping packet for client {}: route {} exceeds max depth",
                                addr,
                                pkt.routing
                            );
                            continue;
                        };
                        pkt.routing = routing;
                        if dump_traffic && is_rpc(&pkt.payload) {
                            log::info!("{}->{} -- {:?}", pkt.routing, addr, pkt.payload);
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

    fn log_device_packet(&self, mut pkt: proto::Packet, prefix: &proto::DeviceRoute) {
        let Ok(routing) = prefix.absolute_route(&pkt.routing) else {
            log::warn!("Dropping packet whose mounted route exceeds max depth");
            return;
        };
        pkt.routing = routing;
        let dump = match pkt.payload {
            proto::Payload::Heartbeat(_) => self.config.dump_hb,
            proto::Payload::Metadata(_) => self.config.dump_meta,
            proto::Payload::StreamData(_) => self.config.dump_data,
            _ => self.config.dump_traffic,
        };
        if dump {
            log::info!("Packet from {} -- {:?}", pkt.routing, pkt.payload);
        }
        if let proto::Payload::LogMessage(log_msg) = pkt.payload {
            // Map the device-reported level onto the log crate's level
            // so the logger filter and prefix reflect it.
            let level = match &log_msg.level {
                proto::LogLevel::Critical | proto::LogLevel::Error => log::Level::Error,
                proto::LogLevel::Warning => log::Level::Warn,
                proto::LogLevel::Info => log::Level::Info,
                proto::LogLevel::Debug => log::Level::Debug,
                proto::LogLevel::Unknown(_) => log::Level::Info,
            };
            log::log!(target: &format!("device::{}", pkt.routing), level, "{}", log_msg.message);
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
/// discover this proxy (e.g. via `tio list`).
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

/// Discover the device routes on `interface` and return the `dev.name` of each,
/// over the one shared connection.
#[cfg(feature = "mdns")]
fn discover_sensor_models(interface: &proxy::Interface) -> Vec<String> {
    let Ok(mut tree) = DeviceTree::open(interface, proto::DeviceRoute::root()) else {
        return Vec::new();
    };
    tree.named_routes(MDNS_ROUTE_WINDOW)
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
) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
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
    Ok(())
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

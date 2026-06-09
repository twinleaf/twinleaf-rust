//! tio proxy
//!
//! Multiplexes access to one or more sensors, exposing the functionality of
//! tio::proxy via TCP. With `--mount`, each sensor hangs off a route prefix
//! and the proxy presents the set as a single virtual hub.

use crate::{MountArg, ProxyCli, ProxySubcommands};
use std::io;
use std::net::TcpListener;
use std::time::Duration;
use twinleaf::device::discovery::{self, PortInterface};
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
            crate::tools::proxy_nmea::run_nmea_proxy(tio, tcp_port)
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
                return crate::tools::list::list_devices_deprecated(true);
            }
            if proxy_cli.auto {
                log::warn!(
                    "'--auto' is deprecated; running without a URL now auto-detects by default"
                );
            }
            let mounts = std::mem::take(&mut proxy_cli.mounts);
            let layout = Layout::from_cli(mounts, proxy_cli.sensor_url.take())?;
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
            reconnect_timeout: Duration::from_secs(cli.reconnect_timeout),
            disconnect_slow: cli.kick_slow,
            verbose: cli.verbose,
            debugging: cli.debug,
            subtree: cli.subtree.clone(),
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
    fn from_cli(mount_args: Vec<MountArg>, sensor_url: Option<String>) -> eyre::Result<Layout> {
        if mount_args.is_empty() {
            return Ok(Layout {
                mounts: vec![resolve_root_mount(sensor_url)?],
            });
        }
        let mut prefixes = std::collections::HashSet::new();
        for arg in &mount_args {
            if !prefixes.insert(arg.prefix.clone()) {
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

/// Resolve the sensor URL into a root-prefix `Mount`, auto-detecting if no
/// URL was given.
fn resolve_root_mount(sensor_url: Option<String>) -> eyre::Result<Mount> {
    use color_eyre::Help;

    let auto_detected = sensor_url.is_none();
    let locator = if let Some(url) = sensor_url {
        url
    } else {
        let devices = discovery::enumerate_serial(false);
        let mut valid_urls = Vec::new();
        for dev in devices {
            match dev.interface {
                PortInterface::STM32 | PortInterface::FTDI => {
                    valid_urls.push(dev.url.clone());
                }
                _ => {}
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
        valid_urls.swap_remove(0)
    };

    Ok(Mount {
        locator,
        prefix: proto::DeviceRoute::root(),
        auto_detected,
    })
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

        let mut links = Vec::with_capacity(self.layout.mounts.len());
        for mount in &self.layout.mounts {
            let (status_send, status_rx) = crossbeam::channel::bounded::<proxy::Event>(100);
            let interface = proxy::Interface::new_proxy(
                &mount.locator,
                Some(self.config.reconnect_timeout),
                Some(status_send),
            );
            // This is used by the proxy itself to communicate with the device
            // tree, for now only to receive log messages and dump traffic.
            let monitor_port = match interface.subtree_full(self.config.subtree.clone()) {
                Ok(port) => port,
                Err(e) => {
                    let last_status = status_rx.iter().last();
                    let err = eyre::Report::new(e)
                        .wrap_err(format!("could not open port on {}", mount.locator));
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
                prefix: mount.prefix.clone(),
                interface,
                status_rx,
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
            tio::port::Port::rx_channel_custom(proxy::Interface::get_client_tx_channel_size());
        let client = match tio::port::Port::from_tcp_stream_custom(
            stream,
            tio::port::Port::rx_to_channel(rx_send),
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
                    self.config.subtree.clone(),
                    usize::MAX,
                    true,
                    true,
                )
                .expect("Failed to create new proxy port");
            ports.push((link.prefix.clone(), port));
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
                        pkt.routing = prefix.absolute_route(&pkt.routing);
                        if pkt.routing.len() > proto::TIO_PACKET_MAX_ROUTING_SIZE {
                            log::warn!(
                                "Dropping packet for client {}: route {} exceeds max depth",
                                addr,
                                pkt.routing
                            );
                            continue;
                        }
                        if dump_traffic && is_rpc(&pkt.payload) {
                            log::info!("{}->{} -- {:?}", pkt.routing, addr, pkt.payload);
                        }
                        match client.try_send(pkt) {
                            Ok(()) => slow.packet_delivered(&addr),
                            Err(tio::SendError::Full) if !disconnect_slow => {
                                slow.packet_dropped(&addr)
                            }
                            Err(tio::SendError::Full) => break Disconnect::TooSlow,
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
        pkt.routing = prefix.absolute_route(&pkt.routing);
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
        proxy::Event::ProtocolError(perr) => match perr {
            proto::Error::Text(txt) => {
                log::info!(target: target, "Text: {}", txt);
            }
            other => {
                log::debug!(target: target, "Protocol error: {:?}", other);
            }
        },
        evt => {
            log::trace!(target: target, "{:?}", evt);
        }
    }
}

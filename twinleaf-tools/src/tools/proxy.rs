//! tio proxy
//!
//! Multiplexes access to a sensor, exposing the functionality of tio::proxy
//! via TCP.

use crate::{ProxyCli, ProxySubcommands};
use std::io;
use std::net::TcpListener;
use std::time::Duration;
use twinleaf::device::discovery::{self, PortInterface};
use twinleaf::tio::{self, proto, proxy};

pub fn run_proxy(mut proxy_cli: ProxyCli) -> eyre::Result<()> {
    match proxy_cli.subcommands.take() {
        Some(ProxySubcommands::Nmea { tio, tcp_port }) => {
            crate::tools::proxy_nmea::run_nmea_proxy(tio, tcp_port)
        }
        None => run_proxy_server(ProxyConfig::from(proxy_cli)),
    }
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    tcp_port: u16,
    reconnect_timeout: Duration,
    disconnect_slow: bool,
    verbose: bool,
    debugging: bool,
    dump_traffic: bool,
    dump_data: bool,
    dump_meta: bool,
    dump_hb: bool,
    timestamp_format: String,
    sensor_url: Option<String>,
    subtree: proto::DeviceRoute,
    auto: bool,
    enumerate: bool,
}

impl From<ProxyCli> for ProxyConfig {
    fn from(proxy_cli: ProxyCli) -> Self {
        Self {
            tcp_port: proxy_cli.port,
            reconnect_timeout: Duration::from_secs(proxy_cli.reconnect_timeout),
            disconnect_slow: proxy_cli.kick_slow,
            verbose: proxy_cli.verbose,
            debugging: proxy_cli.debug,
            dump_traffic: proxy_cli.dump,
            dump_data: proxy_cli.dump_data,
            dump_meta: proxy_cli.dump_meta,
            dump_hb: proxy_cli.dump_hb,
            timestamp_format: proxy_cli.timestamp_format,
            sensor_url: proxy_cli.sensor_url,
            subtree: proxy_cli.subtree,
            auto: proxy_cli.auto,
            enumerate: proxy_cli.enumerate,
        }
    }
}

#[derive(Default)]
struct ProxyState {
    _clients: usize,
    _dropped_packets: usize,
}

struct ResolvedSensor {
    url: String,
    auto_detected: bool,
}

fn print_startup(sensor: &ResolvedSensor, config: &ProxyConfig) {
    println!("tio proxy starting:");
    println!(
        "  Sensor: {} {}",
        sensor.url,
        if sensor.auto_detected {
            "(auto-detected)"
        } else {
            ""
        }
    );
    println!("  TCP port: {}", config.tcp_port);
    println!("  Subtree: {}", config.subtree);

    let flags = startup_flags(config);
    if !flags.is_empty() {
        println!("  Flags: {}", flags.join(" "));
    }
    println!();
}

fn startup_flags(config: &ProxyConfig) -> Vec<&'static str> {
    let mut flags = Vec::new();
    if config.verbose {
        flags.push("verbose");
    }
    if config.debugging {
        flags.push("debug");
    }
    if config.disconnect_slow {
        flags.push("kick-slow");
    }
    if config.dump_traffic {
        flags.push("dump");
    }
    if config.dump_data {
        flags.push("dump-data");
    }
    if config.dump_meta {
        flags.push("dump-meta");
    }
    if config.dump_hb {
        flags.push("dump-hb");
    }
    flags
}

macro_rules! log{
    ($tf:expr, $msg:expr)=>{
    {
        println!("{}{}", chrono::Local::now().format(&$tf), $msg);
    }
    };
    ($tf:expr, $f:expr,$($a:tt)*)=>{
    {
        log!($tf, format!($f, $($a)*));
    }
    };
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

pub fn run_proxy_server(config: ProxyConfig) -> eyre::Result<()> {
    use color_eyre::{Help, SectionExt};
    use eyre::bail;

    // Handle --enum mode (deprecated; now delegates to `tio list`)
    if config.enumerate {
        return crate::tools::list::list_devices_deprecated(true);
    }

    if config.auto {
        eprintln!(
            "warning: '--auto' is deprecated; running without -s <url> now auto-detects by default"
        );
    }

    let mut _state = ProxyState::default();

    // Determine sensor URL; if none given, auto-detect.
    let auto_detected = config.sensor_url.is_none();
    let sensor_url = if let Some(url) = &config.sensor_url {
        url.clone()
    } else {
        // --auto mode
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
        if valid_urls.len() == 0 {
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
                .suggestion("specify one with -s <url>"));
        }
        valid_urls[0].clone()
    };

    let subtree = config.subtree.clone();
    let sensor = ResolvedSensor {
        url: sensor_url,
        auto_detected,
    };
    print_startup(&sensor, &config);

    let new_client = {
        let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);
        let started_v6 = create_listener_thread(
            std::net::SocketAddr::new(
                std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                config.tcp_port,
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
                    config.tcp_port,
                ),
                client_send.clone(),
            )
        };
        if let (Err(e1), Err(e2)) = (started_v6, started_v4) {
            let addr_in_use = matches!(e1.kind(), io::ErrorKind::AddrInUse)
                || matches!(e2.kind(), io::ErrorKind::AddrInUse);
            let err = eyre::eyre!(
                "could not bind TCP port {}: v6={}, v4={}",
                config.tcp_port,
                e1,
                e2
            );
            return Err(if addr_in_use {
                err.suggestion(format!(
                    "another 'tio proxy' is likely running on port {}; try --port <N>",
                    config.tcp_port
                ))
            } else {
                err
            });
        }
        new_client
    };

    let (status_send, port_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let proxy = proxy::Interface::new_proxy(
        &sensor.url,
        Some(config.reconnect_timeout),
        Some(status_send),
    );

    // This is used by the proxy itself to communicate with the device tree.
    // for now only used to receive log messages and dump traffic.
    let proxy_port = match proxy.subtree_full(subtree.clone()) {
        Ok(port) => port,
        Err(e) => {
            let last_status = port_status.iter().last();
            let err =
                eyre::Report::new(e).wrap_err(format!("could not open port on {}", sensor.url));
            return Err(if let Some(status) = last_status {
                err.with_section(move || format!("{:?}", status).header("Last proxy event:"))
            } else {
                err
            });
        }
    };

    use crossbeam::select;
    loop {
        select! {
            recv(new_client) -> tcp_client => {
                if let Ok(stream) = tcp_client {
                    let addr = match stream.peer_addr() {
                        Ok(addr) => addr.to_string(),
                        Err(err) => {
                            log!(
                                config.timestamp_format,
                                "Failed to determine client address: {:?}",
                                err
                            );
                            continue;
                        }
                    };
                    // A client from the proxy perspective is a port in reverse, i.e. what it receives
                    // is what a client transmits, and vice-versa. Therefore, the channel size settings
                    // for rx and tx are inverted. Also, we use the proxy port channel size setting
                    // instead of the physical ports setting.
                    let (rx_send, client_rx) = tio::port::Port::rx_channel_custom(proxy::Interface::get_client_tx_channel_size());
                    let client = match tio::port::Port::from_tcp_stream_custom(stream, tio::port::Port::rx_to_channel(rx_send), proxy::Interface::get_client_rx_channel_size()) {
                        Ok(client_port) => client_port,
                        _ => continue,
                    };

                    if config.verbose {
                        log!(config.timestamp_format, "Accepted client from {}", addr);
                    }
                    let port = proxy.new_port(Some(Duration::from_millis(2000)), subtree.clone(), usize::MAX, true, true).expect("Failed to create new proxy port");
                    let client_config = config.clone();
                    std::thread::spawn(move || {
                        let mut is_slow = false;
                        let mut dropped: usize = 0;
                        loop {
                            select! {
                                recv(port.receiver()) -> res => {
                                    let pkt = if let Ok(pkt) = res { pkt } else {
                                        log!(client_config.timestamp_format, "Disconnecting client {} due to internal error receiving tio data in thread", addr);
                                            break;
                                    };
                                    if client_config.dump_traffic {
                                        if match pkt.payload {
                                            proto::Payload::RpcRequest(_) | proto::Payload::RpcReply(_) | proto::Payload::RpcError(_) => true,
                                            _ => false,
                                        } {
                                            log!(client_config.timestamp_format, "{}->{} -- {:?}", pkt.routing, addr, pkt.payload);
                                        }
                                    }
                                    match client.try_send(pkt) {
                                        Err(tio::SendError::Full) => {
                                            if client_config.disconnect_slow {
                                                log!(client_config.timestamp_format, "Disconnecting client {} due to slowness", addr);
                                                break;
                                            } else if client_config.verbose {
                                                if !is_slow {
                                                    is_slow = true;
                                                    log!(client_config.timestamp_format, "Client {} is not keeping up and is dropping packets", addr);
                                                }
                                                dropped += 1;
                                            }
                                        }
                                        Ok(()) => {
                                            if client_config.verbose && is_slow {
                                                log!(client_config.timestamp_format, "Client {} resuming after having dropped {} packets", addr, dropped);
                                                is_slow = false;
                                                dropped = 0;
                                            }
                                        }
                                        _ => {
                                            if client_config.verbose {
                                                log!(client_config.timestamp_format, "Client {} exiting", addr);
                                            }
                                            break;
                                        }
                                    }
                                }
                                recv(client_rx) -> res => {
                                    match res {
                                        Ok(Ok(pkt)) => {
                                            if client_config.dump_traffic {
                                                log!(client_config.timestamp_format, "{}->{} -- {:?}", addr, pkt.routing, pkt.payload);
                                            }
                                            if let Err(_) = port.try_send(pkt) {
                                                log!(client_config.timestamp_format, "Disconnecting client {} due to internal error forwarding tio data in thread", addr);
                                                    break;
                                            }
                                        }
                                        _ => {
                                            if client_config.verbose {
                                                log!(client_config.timestamp_format, "Client {} exiting", addr);
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    });
                } else {
                    bail!("listener thread died unexpectedly");
                }
            }
            recv(port_status) -> status => {
                if let Ok(evt) = status {
                    match evt {
                        proxy::Event::SensorDisconnected => {
                            log!(config.timestamp_format, "Sensor disconnected");
                        }
                        proxy::Event::SensorReconnected => {
                            log!(config.timestamp_format, "Sensor reconnected");
                        }
                        proxy::Event::FailedToReconnect => {
                            log!(
                                config.timestamp_format,
                                "Stopping reconnection attempts due to timeout"
                            );
                        }
                        proxy::Event::FailedToConnect => {
                            log!(
                                config.timestamp_format,
                                "Fatal proxy error: failed to connect to sensor"
                            );
                        }
                        proxy::Event::FatalError(err) => {
                            log!(config.timestamp_format, "Fatal proxy error: {:?}", err);
                            // the proxy thread will exit and we'll detect it at the next iteration.
                        }
                        proxy::Event::ProtocolError(perr) => {
                            match perr {
                                proto::Error::Text(txt) => {
                                    log!(config.timestamp_format, "Text: {}", txt);
                                }
                                other => {
                                    if config.verbose || config.debugging {
                                        log!(
                                            config.timestamp_format,
                                            "Protocol error: {:?}",
                                            other
                                        );
                                    }
                                }
                            }
                        }
                        evt => {
                            if config.debugging {
                                log!(config.timestamp_format, "Proxy event: {:?}", evt)
                            }
                        }
                    }
                } else {
                    // The proxy thread died, most likely due to the sensor
                    // getting disconnected past the autoreconnection
                    break;
                }
            }
            recv(proxy_port.receiver()) -> pkt_or_err => {
                if let Ok(pkt) = pkt_or_err {
                    let dump = match pkt.payload {
                        proto::Payload::Heartbeat(_) => config.dump_hb,
                        proto::Payload::Metadata(_) => config.dump_meta,
                        proto::Payload::StreamData(_) => config.dump_data,
                        _ => config.dump_traffic
                    };
                    if dump {
                        log!(config.timestamp_format, "Packet from {} -- {:?}", pkt.routing, pkt.payload);
                    }
                    if let proto::Payload::LogMessage(log) = pkt.payload {
                        log!(
                            config.timestamp_format,
                            "{} {:?}: {}",
                            pkt.routing,
                            log.level,
                            log.message
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

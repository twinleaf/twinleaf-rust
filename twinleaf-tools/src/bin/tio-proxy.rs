//! tio-proxy
//!
//! Multiplexes access to a sensor, exposing the functionality of tio::proxy
//! via TCP.

use clap::Parser;
use std::io;
use std::net::TcpListener;
use std::process::ExitCode;
use std::time::Duration;
use tio::{proto, proxy};
use twinleaf::tio;
use twinleaf_tools::ProxyCli;
// Unfortunately we cannot access USB details via the serialport module, so
// we are stuck guessing based on VID/PID. This returns a vector of possible
// serial ports.

#[derive(Debug)]
enum TwinleafPortInterface {
    FTDI,
    STM32,
    Unknown(u16, u16),
}

struct SerialDevice {
    url: String,
    ifc: TwinleafPortInterface,
}

fn enum_devices(all: bool) -> Vec<SerialDevice> {
    let mut ports: Vec<SerialDevice> = Vec::new();

    if let Ok(avail_ports) = serialport::available_ports() {
        for p in avail_ports.iter() {
            if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                let interface = match (info.vid, info.pid) {
                    (0x0403, 0x6015) => TwinleafPortInterface::FTDI,
                    (0x0483, 0x5740) => TwinleafPortInterface::STM32,
                    (vid, pid) => {
                        if !all {
                            continue;
                        };
                        TwinleafPortInterface::Unknown(vid, pid)
                    }
                };
                #[cfg(target_os = "macos")]
                if p.port_name.starts_with("/dev/tty.") && !all {
                    continue;
                }
                ports.push(SerialDevice {
                    url: format!("serial://{}", p.port_name),
                    ifc: interface,
                });
            } // else ignore other types for now: bluetooth, pci, unknown
        }
    }

    ports
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
                    Err(err) => panic!("Error accepting client {:?}", err),
                };
            }
        })?;
    Ok(())
}

fn main() -> ExitCode {
    let cli = ProxyCli::parse();

    macro_rules! die {
        ($f:expr,$($a:tt)*) => {
            die!(format!($f, $($a)*));
        };
        ($msg:expr) => {{
            eprintln!("ERROR: {}", $msg);
            return ExitCode::FAILURE;
        }};
    }

    // Handle --enum mode
    if cli.enumerate {
        let mut unknown_devices = vec![];
        let mut found_any = false;
        for dev in enum_devices(true) {
            if let TwinleafPortInterface::Unknown(vid, pid) = dev.ifc {
                unknown_devices.push(format!("{} (vid: {} pid:{})", dev.url, vid, pid));
            } else {
                if !found_any {
                    println!("Possible tio ports:");
                    found_any = true;
                }
                println!(" * {}", dev.url);
            }
        }
        if !found_any {
            println!("No likely ports found")
        }
        if unknown_devices.len() > 0 {
            println!("Also found these serial ports");
            for dev in unknown_devices {
                println!(" * {}", dev);
            }
        }
        return ExitCode::SUCCESS;
    }

    // Validate sensor_url / --auto combination
    if cli.auto && cli.sensor_url.is_some() {
        die!(
            "both --auto and explicit sensor '{}' given",
            cli.sensor_url.unwrap()
        );
    }
    if !cli.auto && cli.sensor_url.is_none() {
        die!("need sensor url or --auto");
    }

    let tcp_port = cli.port;
    let reconnect_timeout = Duration::from_secs(cli.reconnect_timeout);
    let disconnect_slow = cli.kick_slow;
    let verbose = cli.verbose;
    let debugging = cli.debug;
    let dump_traffic = cli.dump;
    let dump_data = cli.dump_data;
    let dump_meta = cli.dump_meta;
    let dump_hb = cli.dump_hb;
    let tf = cli.timestamp_format;

    // Determine sensor URL
    let sensor_url = if let Some(url) = cli.sensor_url {
        url
    } else {
        // --auto mode
        let devices = enum_devices(false);
        let mut valid_urls = Vec::new();
        for dev in devices {
            match dev.ifc {
                TwinleafPortInterface::STM32 | TwinleafPortInterface::FTDI => {
                    valid_urls.push(dev.url.clone());
                }
                _ => {}
            }
        }
        if valid_urls.len() == 0 {
            die!("Cannot find any sensor to connect to, specify URL manually")
        }
        if valid_urls.len() > 1 {
            die!("Too many sensors detected, specify URL manually")
        }
        valid_urls[0].clone()
    };

    let subtree = tio::proto::DeviceRoute::from_str(&cli.subtree).expect("Invalid sensor subtree");

    println!("tio-proxy starting:");
    println!(
        "  Sensor: {} {}",
        sensor_url,
        if cli.auto { "(auto-detected)" } else { "" }
    );
    println!("  TCP port: {}", tcp_port);
    println!("  Subtree: {}", subtree);
    if verbose || debugging || dump_traffic || dump_data || dump_meta || dump_hb {
        print!("  Flags:");
        if verbose {
            print!(" verbose");
        }
        if debugging {
            print!(" debug");
        }
        if disconnect_slow {
            print!(" kick-slow");
        }
        if dump_traffic {
            print!(" dump");
        }
        if dump_data {
            print!(" dump-data");
        }
        if dump_meta {
            print!(" dump-meta");
        }
        if dump_hb {
            print!(" dump-hb");
        }
        println!();
    }
    println!();

    let new_client = {
        let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);
        let started_v6 = create_listener_thread(
            std::net::SocketAddr::new(
                std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                tcp_port,
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
                    tcp_port,
                ),
                client_send.clone(),
            )
        };
        if let (Err(e1), Err(e2)) = (started_v6, started_v4) {
            die!("Failed to start up server: {:?}/{:?}", e1, e2);
        }
        new_client
    };

    let (status_send, port_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let proxy =
        proxy::Interface::new_proxy(&sensor_url, Some(reconnect_timeout), Some(status_send));

    // This is used by the proxy itself to communicate with the device tree.
    // for now only used to receive log messages and dump traffic.
    let proxy_port = if let Ok(port) = proxy.subtree_full(subtree.clone()) {
        port
    } else {
        die!(
            "Failed to open port{}",
            match port_status.iter().last() {
                Some(status) => format!(": {:?}", status),
                _ => "".to_string(),
            }
        );
    };

    use crossbeam::select;
    loop {
        select! {
            recv(new_client) -> tcp_client => {
                if let Ok(stream) = tcp_client {
                    let addr = match stream.peer_addr() {
                        Ok(addr) => addr.to_string(),
                        Err(err) => {
                            log!(tf, "Failed to determine client address: {:?}", err);
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

                    if verbose {
                        log!(tf, "Accepted client from {}", addr);
                    }
                    let port = proxy.new_port(Some(Duration::from_millis(2000)), subtree.clone(), usize::MAX, true, true).expect("Failed to create new proxy port");
                    let tf = tf.clone();
                    std::thread::spawn(move || {
                        let mut is_slow = false;
                        let mut dropped: usize = 0;
                        loop {
                            select! {
                                recv(port.receiver()) -> res => {
                                    let pkt = if let Ok(pkt) = res { pkt } else {
                                        log!(tf, "Disconnecting client {} due to internal error receiving tio data in thread", addr);
                                            break;
                                    };
                                    if dump_traffic {
                                        if match pkt.payload {
                                            proto::Payload::RpcRequest(_) | proto::Payload::RpcReply(_) | proto::Payload::RpcError(_) => true,
                                            _ => false,
                                        } {
                                            log!(tf, "{}->{} -- {:?}", pkt.routing, addr, pkt.payload);
                                        }
                                    }
                                    match client.try_send(pkt) {
                                        Err(tio::SendError::Full) => {
                                            if disconnect_slow {
                                                log!(tf, "Disconnecting client {} due to slowness", addr);
                                                break;
                                            } else if verbose {
                                                if !is_slow {
                                                    is_slow = true;
                                                    log!(tf, "Client {} is not keeping up and is dropping packets", addr);
                                                }
                                                dropped += 1;
                                            }
                                        }
                                        Ok(()) => {
                                            if verbose && is_slow {
                                                log!(tf, "Client {} resuming after having dropped {} packets", addr, dropped);
                                                is_slow = false;
                                                dropped = 0;
                                            }
                                        }
                                        _ => {
                                            if verbose {
                                                log!(tf, "Client {} exiting", addr);
                                            }
                                            break;
                                        }
                                    }
                                }
                                recv(client_rx) -> res => {
                                    match res {
                                        Ok(Ok(pkt)) => {
                                            if dump_traffic {
                                                log!(tf, "{}->{} -- {:?}", addr, pkt.routing, pkt.payload);
                                            }
                                            if let Err(_) = port.try_send(pkt) {
                                                log!(tf, "Disconnecting client {} due to internal error forwarding tio data in thread", addr);
                                                    break;
                                            }
                                        }
                                        _ => {
                                            if verbose {
                                                log!(tf, "Client {} exiting", addr);
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    });
                } else {
                    die!("Listener thread died unexpectedly");
                }
            }
            recv(port_status) -> status => {
                if let Ok(evt) = status {
                    match evt {
                        proxy::Event::SensorDisconnected => {
                            log!(tf, "Sensor disconnected");
                        }
                        proxy::Event::SensorReconnected => {
                            log!(tf, "Sensor reconnected");
                        }
                        proxy::Event::FailedToReconnect => {
                            log!(tf, "Stopping reconnection attempts due to timeout");
                        }
                        proxy::Event::FailedToConnect => {
                            log!(tf, "Fatal proxy error: failed to connect to sensor");
                        }
                        proxy::Event::FatalError(err) => {
                            log!(tf, "Fatal proxy error: {:?}", err);
                            // the proxy thread will exit and we'll detect it at the next iteration.
                        }
                        proxy::Event::ProtocolError(perr) => {
                            match perr {
                                proto::Error::Text(txt) => {
                                    log!(tf, "Text: {}", txt);
                                }
                                other => {
                                    if verbose || debugging {
                                        log!(tf, "Protocol error: {:?}", other);
                                    }
                                }
                            }
                        }
                        evt => {
                            if debugging {
                                log!(tf, "Proxy event: {:?}", evt)
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
                        proto::Payload::Heartbeat(_) => dump_hb,
                        proto::Payload::Metadata(_) => dump_meta,
                        proto::Payload::StreamData(_) => dump_data,
                        _ => dump_traffic
                    };
                    if dump {
                        log!(tf, "Packet from {} -- {:?}", pkt.routing, pkt.payload);
                    }
                    if let proto::Payload::LogMessage(log) = pkt.payload {
                        log!(tf, "{} {:?}: {}", pkt.routing, log.level, log.message);
                    }
                }
            }
        }
    }

    ExitCode::SUCCESS
}

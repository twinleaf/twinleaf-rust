//! tio proxy
//!
//! Multiplexes access to a sensor, exposing the functionality of tio::proxy
//! via TCP.

use crate::ProxyCli;
use std::io;
use std::net::TcpListener;
use std::time::Duration;
use tio::{proto, proxy};
use twinleaf::device::discovery::{self, PortInterface};
use twinleaf::tio;

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

pub fn run_proxy(proxy_cli: ProxyCli) -> Result<(), ()> {
    macro_rules! die {
        ($f:expr,$($a:tt)*) => {
            die!(format!($f, $($a)*));
        };
        ($msg:expr) => {{
            eprintln!("ERROR: {}", $msg);
            return Err(());
        }};
    }

    // Handle --enum mode
    if proxy_cli.enumerate {
        let mut unknown_devices = vec![];
        let mut found_any = false;
        let query_timeout = Duration::from_millis(500);
        for dev in discovery::enumerate_serial(true) {
            if let PortInterface::Unknown(vid, pid) = dev.interface {
                unknown_devices.push(format!("{} (vid: {} pid:{})", dev.url, vid, pid));
            } else {
                if !found_any {
                    println!("Possible tio ports:");
                    found_any = true;
                }
                match discovery::query_name(&dev.url, query_timeout) {
                    Some(name) => println!(" * {}  {}", dev.url, name),
                    None => println!(" * {}  (no response)", dev.url),
                }
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
        return Ok(());
    }

    let tcp_port = proxy_cli.port;
    let reconnect_timeout = Duration::from_secs(proxy_cli.reconnect_timeout);
    let disconnect_slow = proxy_cli.kick_slow;
    let verbose = proxy_cli.verbose;
    let debugging = proxy_cli.debug;
    let dump_traffic = proxy_cli.dump;
    let dump_data = proxy_cli.dump_data;
    let dump_meta = proxy_cli.dump_meta;
    let dump_hb = proxy_cli.dump_hb;
    let tf = proxy_cli.timestamp_format;

    // Determine sensor URL
    let sensor_url = if let Some(url) = proxy_cli.sensor_url {
        url
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
            die!("Cannot find any sensor to connect to, specify URL manually")
        }
        if valid_urls.len() > 1 {
            eprintln!("ERROR: multiple sensors detected:");
            let query_timeout = Duration::from_millis(500);
            for url in &valid_urls {
                match discovery::query_name(url, query_timeout) {
                    Some(name) => eprintln!("  {}  {}", url, name),
                    None => eprintln!("  {}  (no response)", url),
                }
            }
            eprintln!("Specify one with -s <url>.");
            return Err(());
        }
        valid_urls[0].clone()
    };

    let subtree = proxy_cli.subtree;

    println!("tio proxy starting:");
    println!(
        "  Sensor: {} {}",
        sensor_url,
        if proxy_cli.auto {
            "(auto-detected)"
        } else {
            ""
        }
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
    Ok(())
}

//! tio-proxy
//!
//! Multiplexes access to a sensor, exposing the functionality of tio::proxy
//! via TCP.

use getopts::Options;
use std::env;
use std::io;
use std::net::TcpListener;
use std::process::ExitCode;
use std::time::Duration;
use tio::{proto, proxy};
use twinleaf::tio;

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
    let mut opts = Options::new();
    opts.optopt(
        "p",
        "",
        "TCP port to listen on for clients (default 7855)",
        "port",
    );
    opts.optflag(
        "k",
        "",
        "Kick off slow clients, instead of dropping traffic.",
    );
    opts.optopt("s", "", "Sensor subtree to look at (default /)", "path");
    opts.optflag("v", "", "Verbose output");
    opts.optflag("d", "", "Debugging output");
    opts.optopt("t", "", "Timestamp format (default '%T%.3f ')", "fmt");
    opts.optopt(
        "T",
        "",
        "Time limit for sensor reconnection attempts (default: 30)",
        "seconds",
    );
    opts.optflag(
        "",
        "dump",
        "Dump traffic data through the proxy (does not include internal heartbeats)",
    );
    opts.optflag("", "auto", "Automatically connect to a USB sensor if there is a single device on the system that could be a Twinleaf device");
    opts.optflag("", "enum", "Enumerate all serial devices, then quit");

    let mut args: Vec<String> = env::args().collect();

    macro_rules! die{
        ($f:expr,$($a:tt)*)=>{
        {
            die!(format!($f, $($a)*));
        }
        };
        ($msg:expr)=>{
        {
            eprintln!("ERROR: {}", $msg);
            return ExitCode::FAILURE;
        }
        };
    }
    macro_rules! die_usage{
        ($f:expr,$($a:tt)*)=>{
        {
            die_usage!(format!($f, $($a)*));
        }
        };
        ($msg:expr)=>{
        {
            let usage = format!("Usage: {} [-p port] [-v] [-d] [-t fmt] (--auto | sensor_url)  or {} --enum", &args[0], &args[0]);
            die!("{}\n{}", $msg, opts.usage(&usage));
        }
        };
    }

    let matches = match opts.parse(&mut args[1..]) {
        Ok(m) => m,
        Err(f) => die_usage!("{}", f.to_string()),
    };

    if matches.opt_present("enum") {
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

    let tcp_port = matches.opt_str("p").unwrap_or("7855".to_string());
    let tcp_port = if let Ok(port) = tcp_port.parse::<u16>() {
        port
    } else {
        die_usage!("Invalid port '{}'", tcp_port);
    };

    let auto_sensor = matches.opt_present("auto");

    let reconnect_timeout = matches.opt_str("T").unwrap_or("30".to_string());
    let reconnect_timeout = if let Ok(t) = reconnect_timeout.parse::<u64>() {
        std::time::Duration::from_secs(t)
    } else {
        die_usage!("Invalid reconnection_timeout '{}'", reconnect_timeout);
    };

    let disconnect_slow = matches.opt_present("k");

    let verbose = matches.opt_present("v");
    let debugging = matches.opt_present("d");
    let dump_traffic = matches.opt_present("dump");
    let tf = matches.opt_str("t").unwrap_or("%T%.3f ".to_string());

    if (matches.free.len() == 0) && !auto_sensor {
        die_usage!("need sensor url or --auto");
    }
    if matches.free.len() > 1 {
        die_usage!("This program supports only a single sensor")
    }
    if (matches.free.len() == 1) && auto_sensor {
        die_usage!(
            "both --auto and explicit sensor '{}' given",
            matches.free[0]
        );
    }

    let sensor_url = if matches.free.len() == 1 {
        matches.free[0].clone()
    } else {
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

    if verbose || auto_sensor {
        log!(tf, "Using sensor url: {}", sensor_url);
    }

    let subtree = if let Some(path) = matches.opt_str("s") {
        tio::proto::DeviceRoute::from_str(&path).expect("Invalid sensor subtree")
    } else {
        tio::proto::DeviceRoute::root()
    };

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

    let (status_send, port_status) = crossbeam::channel::bounded::<proxy::Event>(10);
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
                    let (rx_send, client_rx) = tio::port::Port::rx_channel();
                    let client = match tio::port::Port::from_tcp_stream(stream, tio::port::Port::rx_to_channel(rx_send)) {
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
                    if dump_traffic {
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

//! tio-proxy
//!
//! TODO: rustdoc

use getopts::Options;
use std::env;
use std::net::TcpListener;
use std::process::ExitCode;
use tio::proto;
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
    opts.optflag("", "auto", "automatically connect to a USB sensor if there is a single device on the system that could be a Twinleaf device");

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
            let usage = format!("Usage: {} [-p port] [-v] [-d] [-t fmt] (--auto | sensor_url)", &args[0]);
            die!("{}\n{}", $msg, opts.usage(&usage));
        }
        };
    }

    let matches = match opts.parse(&mut args[1..]) {
        Ok(m) => m,
        Err(f) => die_usage!("{}", f.to_string()),
    };

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

    let listener = TcpListener::bind(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        tcp_port,
    ));
    let listener = match listener {
        Ok(l) => l,
        Err(err) => {
            die!("Failed to bind server: {:?}", err);
        }
    };

    let (status_send, port_status) = crossbeam::channel::bounded::<tio::ProxyEvent>(10);
    let proxy = tio::ProxyPort::new(&sensor_url, Some(reconnect_timeout), Some(status_send));

    // These are used by the proxy itself to communicate with the
    // device tree.
    // for now only used to receive log messages and dump traffic.
    let (_proxy_tx, proxy_rx) = if let Ok(port) = proxy.full_port() {
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

    let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);

    if let Err(err) = std::thread::Builder::new()
        .name("listener".to_string())
        .spawn(move || {
            for res in listener.incoming() {
                match res {
                    Ok(stream) => client_send.send(stream).expect("New client queue full"),
                    Err(err) => panic!("Error accepting client {:?}", err),
                };
            }
        })
    {
        die!("Failed to start up listener thread {:?}", err)
    }

    use crossbeam::select;
    loop {
        select! {
            recv(new_client) -> tcp_client => {
                if let Ok(stream) = tcp_client {
                    let addr = stream.peer_addr().unwrap().to_string();
                    let (rx_send, client_rx) = tio::Port::rx_channel();
                    let client = match tio::Port::from_tcp_stream(stream, tio::Port::rx_to_channel(rx_send)) {
                        Ok(client_port) => client_port,
                        _ => continue,
                    };

                    if verbose {
                        log!(tf, "Accepted client from {}", addr);
                    }
                    let (sender, receiver) = proxy.full_port().unwrap();
                    let tf = tf.clone();
                    std::thread::spawn(move || {
                        let mut is_slow = false;
                        let mut dropped: usize = 0;
                        loop {
                            select! {
                                recv(receiver) -> res => {
                                    let pkt = res.unwrap(); // TODO: this will kill this thread
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
                                            sender.try_send(pkt).unwrap();// TODO
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
                        tio::ProxyEvent::SensorDisconnected => {
                            log!(tf, "Sensor disconnected");
                        }
                        tio::ProxyEvent::SensorReconnected => {
                            log!(tf, "Sensor reconnected");
                        }
                        tio::ProxyEvent::FailedToReconnect => {
                            log!(tf, "Stopping reconnection attempts due to timeout");
                        }
                        tio::ProxyEvent::FailedToConnect => {
                            log!(tf, "Fatal proxy error: failed to connect to sensor");
                        }
                        tio::ProxyEvent::FatalError(err) => {
                            log!(tf, "Fatal proxy error: {:?}", err);
                            // the proxy thread will exit and we'll detect it at the next iteration.
                        }
                        tio::ProxyEvent::ProtocolError(perr) => {
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
            recv(proxy_rx) -> pkt_or_err => {
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

use tio::proto;
use twinleaf::tio;

use std::env;
use std::net::TcpListener;

use getopts::Options;

// Unfortunately we cannot access USB details via the serialport module, so
// we are stuck guessing based on VID/PID. This returns a vector of possible
// serial ports.

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

fn log_msg(desc: &str, what: &tio::Packet) -> String {
    format!(
        "{:?} {}  -- {:?}",
        std::time::Instant::now(),
        desc,
        what.payload
    )
}

fn log_msg2(desc: &str) {
    println!("{:?} {}", std::time::Instant::now(), desc)
}

fn rpc(args: &[String]) -> std::io::Result<()> {
    let mut opts = Options::new();
    opts.optopt(
        "r",
        "",
        "sensor root (default tcp://localhost:7855)",
        "address",
    );
    let matches = match opts.parse(args) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let root = if let Some(url) = matches.opt_str("r") {
        url
    } else {
        "tcp://localhost:7855".to_string()
    };

    if matches.free.len() != 1 {
        // Can only get for now
        panic!("TODO")
    }

    let (port_rx_send, port_rx) = tio::Port::rx_channel();
    let port = tio::Port::from_url(&root, tio::Port::rx_to_channel(port_rx_send))?;
    port.send(tio::Packet::rpc(matches.free[0].clone(), &[]));

    loop {
        // TODO: timeout
        let pkt = match port_rx.recv() {
            Ok(Ok(pkt)) => pkt,
            e => {
                println!("Exiting due to error: {:?}", e);
                return Ok(());
            }
        };
        let mut routing = pkt
            .routing
            .iter()
            .fold(String::new(), |acc, hop| acc + &format!("/{}", hop));
        if routing.len() != 0 {
            continue;
        }
        match pkt.payload {
            proto::Payload::RpcReply(rep) => {
                let human_readable = if let Ok(s) = std::str::from_utf8(&rep.reply) {
                    s
                } else {
                    ""
                };
                println!("Reply: **{}** {:?}", human_readable, rep.reply);
                break;
            }
            proto::Payload::RpcError(err) => {
                println!("Rpc Error: {:?}", err.error);
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

fn proxy(args: &[String]) {
    let mut opts = Options::new();
    opts.optopt(
        "p",
        "",
        "TCP port to listen on for clients (default 7855)",
        "port",
    );
    opts.optflag(
        "v",
        "",
        "Verbose/debug printout of data through the proxy (does not include internal heartbeats)",
    );
    opts.optflag("", "auto", "automatically connect to a USB sensor if there is a single device on the system that could be a Twinleaf device");
    let matches = match opts.parse(args) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let tcp_port = if let Some(p) = matches.opt_str("p") {
        p
    } else {
        "7855".to_string()
    };
    let tcp_port = if let Ok(p) = tcp_port.parse::<u16>() {
        p
    } else {
        panic!("Invalid port {}", tcp_port);
    };
    let verbose = matches.opt_present("v");
    let auto_sensor = matches.opt_present("auto");

    if matches.free.len() > 1 {
        panic!("This program supports only a single sensor")
    }

    if (matches.free.len() == 1) && auto_sensor {
        panic!("auto+explicit sensor given");
    }
    if (matches.free.len() == 0) && !auto_sensor {
        panic!("need sensor url or --auto");
    }

    let sensor_url = if matches.free.len() == 1 {
        matches.free[0].clone()
    } else {
        let devices = enum_devices(false);
        if devices.len() == 0 {
            panic!("Cannot find sensor to connect to, specify URL manually")
        }
        if devices.len() > 1 {
            panic!("Too many sensors detected, specify URL manually")
        }
        devices[0].url.clone()
    };

    println!("Using sensor: {}", sensor_url);

    let listener = TcpListener::bind(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        tcp_port,
    ))
    .unwrap();

    let port = tio::TioProxyPort::new(&sensor_url);

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            _ => continue,
        };
        let (rx_send, client_rx) = tio::Port::rx_channel();
        let client = match tio::Port::from_tcp_stream(stream, tio::Port::rx_to_channel(rx_send)) {
            Ok(client_port) => client_port,
            _ => continue,
        };

        let (sender, receiver) = port.new_proxy();
        std::thread::spawn(move || {
            use crossbeam::select;
            loop {
                select! {
                    recv(receiver) -> res => {
                        let pkt = res.unwrap(); // port failing will close program
                        if verbose {
                            println!("{}", log_msg("port->client", &pkt));
                        }
                        client.send(pkt);
                    }
                    recv(client_rx) -> res => {
                        match res {
                            Ok(Ok(pkt)) => {
                                if verbose {
                                    println!("{}", log_msg("client->port", &pkt));
                                }
                                sender.send(pkt);
                            }
                            _ => {
                                // client failing will listen for the next client
                                println!("Client exiting PROXY");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("help".to_string());
    }
    match args[1].as_str() {
        "proxy" => {
            proxy(&args[2..]); //.unwrap();
        }
        "rpc" => {
            rpc(&args[2..]).unwrap();
        }
        _ => {
            // TODO: do usage right
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool proxy [-p port] [device-url]");
            println!(" tio-tool rpc [-r url] <rpc-name> [rpc-arg]");
        }
    }
}

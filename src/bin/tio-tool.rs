use tio::proto;
use tio::tio_addr;
use twinleaf::tio;

use std::env;
use std::net::TcpListener;

use getopts::Options;

fn random_stuff() {
    //let port = tio::TioPort::new(tio::serial::Port::new("/dev/ttyUSB0").unwrap()).unwrap();
    let port =
        tio::TioPort::new(tio::tcp::Port::new(&tio_addr("127.0.0.1").unwrap()).unwrap()).unwrap();
    //let port = TioPort::new(TioUDP::new(&tio_addr("tio-SYNC8.local").unwrap()).unwrap()).unwrap();
    let mut send_counter = 0u32;
    loop {
        let pkt = match port.recv() {
            Ok(pkt) => pkt,
            Err(e) => {
                println!("Exiting due to error: {:?}", e);
                break;
            }
        };
        let mut routing = pkt
            .routing
            .iter()
            .fold(String::new(), |acc, hop| acc + &format!("/{}", hop));
        if routing.len() == 0 {
            routing = "/".to_string();
            match send_counter {
                20 => {
                    port.send(tio::Packet::rpc("dev.desc".to_string(), &[]));
                    send_counter += 1;
                }
                21 => { /*break;*/ }
                _ => send_counter += 1,
            }
        }
        match pkt.payload {
            proto::Payload::StreamData(sample) => {
                println!(
                    "{}: Stream {}: sample {} {:?}",
                    routing, sample.stream_id, sample.sample_n, sample.data
                );
            }
            proto::Payload::RpcReply(rep) => {
                let human_readable = if let Ok(s) = std::str::from_utf8(&rep.reply) {
                    s
                } else {
                    ""
                };
                println!("Reply: **{}** {:?}", human_readable, rep.reply);
                break;
            }
            proto::Payload::RpcError(_) => {
                println!("Rpc Error todo");
                break;
            }
            proto::Payload::RpcRequest(_) => {
                println!("Request???");
            }
            proto::Payload::Heartbeat(_) => {
                println!("Heartbeat");
            }
            proto::Payload::Unknown(unk) => {
                println!(
                    "Unknown packet, type {} size {}",
                    unk.packet_type,
                    unk.payload.len()
                );
            }
        }
    }
}

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

// this is wrong in so many ways
fn seqproxy(args: &[String]) -> std::io::Result<()> {
    let mut opts = Options::new();
    opts.optopt(
        "p",
        "",
        "TCP port to listen on for clients (default 7855)",
        "PORT",
    );
    opts.optflag(
        "v",
        "",
        "Verbose/debug printout of data through the proxy (does not include internal heartbeats)",
    );
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
    let verbose: bool = matches.opt_present("v");

    if matches.free.len() > 1 {
        panic!("This program supports only a single sensor")
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

    use crossbeam::select;

    let port = tio::TioPort::from_url(&sensor_url)?;
    let listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port))?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nonblocking(true);
        let client = tio::tcp::Port::from_stream(mio::net::TcpStream::from_std(stream));
        let client = match tio::TioPort::new(client?) {
            Ok(port) => port,
            _ => continue,
        };

        // Initial drain of port buffer
        while !port.rx.is_empty() {
            port.recv();
        }

        loop {
            select! {
                recv(port.rx) -> res => {
                    let pkt = res.unwrap().unwrap(); // port failing will close program
                    if verbose {
                    println!("{}", tio::log_msg("port->client", &pkt));
                    }
                    client.send(pkt);
                }
                recv(client.rx) -> res => {
                    match res {
                        Ok(Ok(pkt)) => {
                            if verbose {
                            println!("{}", tio::log_msg("client->port", &pkt));
                            }
                            port.send(pkt);
                        }
                        _ => {
                            // client failing will listen for the next client
                            println!("Client exiting");
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(())
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

    let port = tio::TioPort::from_url(&root)?;
    port.send(tio::Packet::rpc(matches.free[0].clone(), &[]));

    loop {
        // TODO: timeout
        let pkt = match port.recv() {
            Ok(pkt) => pkt,
            Err(e) => {
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

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("help".to_string());
    }
    match args[1].as_str() {
        "proxy" => {
            seqproxy(&args[2..]).unwrap();
        }
        "test" => {
            random_stuff();
        }
        "rpc" => {
            rpc(&args[2..]).unwrap();
        }
        default => {
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool proxy [-p port] [device-url]");
            println!(" tio-tool rpc [-r url] <rpc-name> [rpc-arg]");
        }
    }
}

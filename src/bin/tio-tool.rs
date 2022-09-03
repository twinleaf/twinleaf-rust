use tio::proto;
use tio::proto::DeviceRoute;
use twinleaf::tio;

use std::env;
use std::net::TcpListener;

use getopts::Options;

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

struct RpcMeta {
    arg_type: String,
    size: usize,
    read: bool,
    write: bool,
    persistent: bool,
    unknown: bool,
}

impl RpcMeta {
    pub fn parse(meta: u16) -> RpcMeta {
        let size = ((meta >> 4) & 0xF) as usize;
        let atype = meta & 0xF;
        RpcMeta {
            arg_type: match atype {
                0 => match size {
                    1 => "u8",
                    2 => "u16",
                    4 => "u32",
                    8 => "u64",
                    _ => "",
                },
                1 => match size {
                    1 => "i8",
                    2 => "i16",
                    4 => "i32",
                    8 => "i64",
                    _ => "",
                },
                2 => match size {
                    4 => "f32",
                    8 => "f64",
                    _ => "",
                },
                3 => "string",
                _ => "",
            }
            .to_string(),
            size: size,
            read: (meta & 0x0100) != 0,
            write: (meta & 0x0200) != 0,
            persistent: (meta & 0x0400) != 0,
            unknown: meta == 0,
        }
    }

    pub fn type_str(&self) -> String {
        if (self.arg_type == "string") && (self.size != 0) {
            format!("string<{}>", self.size)
        } else {
            self.arg_type.clone()
        }
    }

    pub fn perm_str(&self) -> String {
        if self.unknown {
            "???".to_string()
        } else {
            format!(
                "{}{}{}",
                if self.read { "R" } else { "-" },
                if self.write { "W" } else { "-" },
                if self.persistent { "P" } else { "-" }
            )
        }
    }
}

fn rpc_get_response(rx: &crossbeam::channel::Receiver<proto::Packet>) -> Result<tio::Packet, ()> {
    loop {
        let pkt = match rx.recv() {
            Ok(pkt) => pkt,
            Err(_) => {
                return Err(());
            }
        };
        match pkt.payload {
            proto::Payload::RpcReply(_) | proto::Payload::RpcError(_) => {
                return Ok(pkt);
            }
            _ => {}
        }
    }
}

fn rpc_get_reply(
    rx: &crossbeam::channel::Receiver<proto::Packet>,
    expected_len: usize,
) -> Result<Vec<u8>, ()> {
    if let Ok(pkt) = rpc_get_response(rx) {
        match pkt.payload {
            proto::Payload::RpcReply(rep) => {
                if (expected_len == 0) || (rep.reply.len() == expected_len) {
                    Ok(rep.reply)
                } else {
                    Err(())
                }
            }
            proto::Payload::RpcError(err) => {
                println!("Rpc Error: {:?}", err.error);
                Err(())
            }
            _ => Err(()),
        }
    } else {
        Err(())
    }
}

fn tio_opts() -> Options {
    let mut opts = Options::new();
    opts.optopt("r", "", "sensor root (default tcp://localhost)", "address");
    opts.optopt(
        "s",
        "",
        "sensor path in the sensor tree (default /)",
        "path",
    );
    opts
}

fn tio_parseopts(opts: Options, args: &[String]) -> (getopts::Matches, String, DeviceRoute) {
    let matches = match opts.parse(args) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let root = if let Some(url) = matches.opt_str("r") {
        url
    } else {
        "tcp://localhost".to_string()
    };
    let route = if let Some(path) = matches.opt_str("s") {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };
    (matches, root, route)
}

fn list_rpcs(args: &[String]) -> std::io::Result<()> {
    let mut opts = tio_opts();
    opts.optflag("l", "", "List RPCs");
    opts.optopt(
        "t",
        "",
        "RPC type (one of u8/u16/u32/u64 i8/i16/i32/i64 f32/f64 string). ",
        "type",
    );
    let (_matches, root, route) = tio_parseopts(opts, args);
    let proxy = tio::TioProxyPort::new(&root, None, None);
    let (tx, rx) = proxy.new_port(None, route, false, false).unwrap();

    tx.send(tio::Packet::rpc("rpc.listinfo".to_string(), &vec![]))
        .unwrap();
    let nrpcs = u16::from_le_bytes(rpc_get_reply(&rx, 2).unwrap()[0..2].try_into().unwrap());

    for rpc_id in 0..nrpcs {
        tx.send(tio::Packet::rpc(
            "rpc.listinfo".to_string(),
            &rpc_id.to_le_bytes(),
        ))
        .unwrap();
        let reply = rpc_get_reply(&rx, 0).unwrap();
        let meta = RpcMeta::parse(u16::from_le_bytes(reply[0..2].try_into().unwrap()));
        let name = String::from_utf8_lossy(&reply[2..]).to_string();
        println!("{} {}({})", meta.perm_str(), name, meta.type_str());
    }

    Ok(())
}

fn get_rpctype(
    name: &String,
    tx: &crossbeam::channel::Sender<proto::Packet>,
    rx: &crossbeam::channel::Receiver<proto::Packet>,
) -> String {
    tx.send(tio::Packet::rpc("rpc.info".to_string(), name.as_bytes()))
        .unwrap();
    RpcMeta::parse(u16::from_le_bytes(
        rpc_get_reply(&rx, 2).unwrap()[0..2].try_into().unwrap(),
    ))
    .arg_type
}

fn rpc(args: &[String]) -> std::io::Result<()> {
    let mut opts = tio_opts();
    opts.optopt(
        "t",
        "",
        "RPC type (one of u8/u16/u32/u64 i8/i16/i32/i64 f32/f64 string). ",
        "type",
    );
    let (matches, root, route) = tio_parseopts(opts, args);

    let rpc_name = if matches.free.len() < 1 {
        panic!("must specify rpc name")
    } else {
        matches.free[0].clone()
    };

    let rpc_arg = if matches.free.len() > 2 {
        panic!("usage: name [arg]")
    } else if matches.free.len() == 2 {
        Some(matches.free[1].clone())
    } else {
        None
    };

    let port = tio::TioProxyPort::new(&root, None, None);
    let (tx, rx) = port.new_port(None, route, false, false).unwrap();

    let mut rpc_type = if let Some(rpc_type) = matches.opt_str("t") {
        Some(rpc_type)
    } else {
        if rpc_arg.is_some() {
            let t = get_rpctype(&rpc_name, &tx, &rx);
            Some(if t == "" { "string".to_string() } else { t })
        } else {
            None
        }
    };

    tx.send(tio::Packet::rpc(
        rpc_name.clone(),
        &if rpc_arg.is_none() {
            vec![]
        } else {
            let s = rpc_arg.unwrap();
            match &rpc_type.as_ref().unwrap()[..] {
                "u8" => s.parse::<u8>().unwrap().to_le_bytes().to_vec(),
                "u16" => s.parse::<u16>().unwrap().to_le_bytes().to_vec(),
                "u32" => s.parse::<u32>().unwrap().to_le_bytes().to_vec(),
                "u64" => s.parse::<u32>().unwrap().to_le_bytes().to_vec(),
                "i8" => s.parse::<i8>().unwrap().to_le_bytes().to_vec(),
                "i16" => s.parse::<i16>().unwrap().to_le_bytes().to_vec(),
                "i32" => s.parse::<i32>().unwrap().to_le_bytes().to_vec(),
                "i64" => s.parse::<i32>().unwrap().to_le_bytes().to_vec(),
                "f32" => s.parse::<f32>().unwrap().to_le_bytes().to_vec(),
                "f64" => s.parse::<f64>().unwrap().to_le_bytes().to_vec(),
                "string" => s.as_bytes().to_vec(),
                _ => panic!("Invalid type"),
            }
        },
    ))
    .unwrap();

    let reply = rpc_get_reply(&rx, 0).unwrap();
    if reply.len() != 0 {
        if rpc_type.is_none() {
            let t = get_rpctype(&rpc_name, &tx, &rx);
            rpc_type = Some(if t == "" { "string".to_string() } else { t })
        }
        let reply_str = match &rpc_type.as_ref().unwrap()[..] {
            "u8" => u8::from_le_bytes(reply[0..1].try_into().unwrap()).to_string(),
            "u16" => u16::from_le_bytes(reply[0..2].try_into().unwrap()).to_string(),
            "u32" => u32::from_le_bytes(reply[0..4].try_into().unwrap()).to_string(),
            "u64" => u64::from_le_bytes(reply[0..8].try_into().unwrap()).to_string(),
            "i8" => i8::from_le_bytes(reply[0..1].try_into().unwrap()).to_string(),
            "i16" => i16::from_le_bytes(reply[0..2].try_into().unwrap()).to_string(),
            "i32" => i32::from_le_bytes(reply[0..4].try_into().unwrap()).to_string(),
            "i64" => i64::from_le_bytes(reply[0..8].try_into().unwrap()).to_string(),
            "f32" => f32::from_le_bytes(reply[0..4].try_into().unwrap()).to_string(),
            "f64" => f64::from_le_bytes(reply[0..8].try_into().unwrap()).to_string(),
            "string" => format!(
                "\"{}\" {:?}",
                if let Ok(s) = std::str::from_utf8(&reply) {
                    s
                } else {
                    ""
                },
                reply
            ),
            _ => panic!("Invalid type"),
        };
        println!("Reply: {}", reply_str);
    }
    println!("OK");
    Ok(())
}

static mut _TIME_FMT: String = String::new();

fn set_global_timefmt(fmt: String) {
    unsafe {
        _TIME_FMT = fmt;
    }
}

fn global_timefmt() -> &'static str {
    unsafe { &_TIME_FMT }
}

macro_rules! log{
    ($f:expr,$($a:tt)*)=>{
       {
           println!(concat!("{}", $f),chrono::Local::now().format(global_timefmt()), $($a)*)
       }
    }
}

fn proxy(args: &[String]) {
    let mut opts = Options::new();
    opts.optopt(
        "p",
        "",
        "TCP port to listen on for clients (default 7855)",
        "port",
    );
    opts.optflag("v", "", "Verbose/debug printout of proxy events");
    opts.optflag(
        "d",
        "",
        "Dump traffic data through the proxy (does not include internal heartbeats)",
    );
    opts.optopt("t", "", "Timestamp format (default '%T%.3f ')", "port");
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
    let dump_traffic = matches.opt_present("d");
    let auto_sensor = matches.opt_present("auto");

    if let Some(t) = matches.opt_str("t") {
        set_global_timefmt(t);
    } else {
        set_global_timefmt("%T%.3f ".to_string());
    };

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
            panic!("Cannot find sensor to connect to, specify URL manually")
        }
        if valid_urls.len() > 1 {
            panic!("Too many sensors detected, specify URL manually")
        }
        valid_urls[0].clone() // TODO
    };

    log!("Using sensor url: {}", sensor_url);

    let listener = TcpListener::bind(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        tcp_port,
    ))
    .unwrap();

    let (status_send, port_status) = crossbeam::channel::bounded::<tio::TioProxyEvent>(10);
    let port = tio::TioProxyPort::new(
        &sensor_url,
        Some(std::time::Duration::from_secs(30)),
        Some(status_send),
    );

    // These are used by the proxy itself to communicate with the
    // device tree.
    // for now only used to receive log messages and dump traffic.
    let (_proxy_tx, proxy_rx) = port.port().unwrap();

    let (client_send, new_client) = crossbeam::channel::bounded::<std::net::TcpStream>(10);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => client_send.send(s).unwrap(), // TODO
                _ => continue,
            };
        }
    });

    use crossbeam::select;
    loop {
        select! {
            recv(new_client) -> tcp_client => {
                match tcp_client {
                    Ok(stream) => {
                        let addr = stream.peer_addr().unwrap().to_string();
                        let (rx_send, client_rx) = tio::Port::rx_channel();
                        let client = match tio::Port::from_tcp_stream(stream, tio::Port::rx_to_channel(rx_send)) {
                            Ok(client_port) => client_port,
                            _ => continue,
                        };

                        if verbose {
                            log!("Accepted client from {}", addr);
                        }
                        let (sender, receiver) = port.port().unwrap();
                        std::thread::spawn(move || {
                            loop {
                                select! {
                                    recv(receiver) -> res => {
                                        let pkt = res.unwrap(); // port failing will close program
                                        client.send(pkt).unwrap(); // TODO
                                    }
                                    recv(client_rx) -> res => {
                                        match res {
                                            Ok(Ok(pkt)) => {
                                                if dump_traffic {
                                                    log!("{}->{} -- {:?}", addr, pkt.routing, pkt.payload);
                                                }
                                                sender.send(pkt).unwrap();// TODO
                                            }
                                            _ => {
                                                // client failing will listen for the next client
                                                if verbose {
                                                    log!("Client {} exiting", addr);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        });
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            recv(port_status) -> status => {
                match status {
                    Ok(s) => {
                        if verbose {
                            log!("ProxyPort event: {:?}", s);
                        }
                    }
                    Err(crossbeam::RecvError) => {break;}
                }
            }
            recv(proxy_rx) -> pkt_or_err => {
                if let Ok(pkt) = pkt_or_err {
                    if dump_traffic {
                        log!("Packet from {} -- {:?}", pkt.routing, pkt.payload);
                    }
                    if let proto::Payload::LogMessage(log) = pkt.payload {
                        log!("{} {:?}: {}", pkt.routing, log.level, log.message);
                    }
                }
            }
        }
    }
}

fn dump(args: &[String]) {
    let opts = tio_opts();
    let (_matches, root, _route) = tio_parseopts(opts, args);

    // TODO: is there a way to do this with proxy port??
    //let port = tio::TioProxyPort::new(&root, None, None);
    let (rx_send, rx) = tio::Port::rx_channel();
    let _port = tio::Port::from_url(&root, tio::Port::rx_to_channel(rx_send)).unwrap();

    loop {
        match rx.recv() {
            Ok(Ok(pkt)) => {
                println!("PKT: {:?}", pkt);
            }
            Ok(Err(tio::RecvError::Protocol(proto::Error::Text(txt)))) => {
                println!("TXT: {}", txt);
            }
            Ok(Err(tio::RecvError::Protocol(perr))) => {
                println!("PROTO: {:?}", perr);
            }
            reason => {
                println!("BREAKING: {:?}", reason);
                break;
            }
        }
    }
}

fn firmware_upgrade(args: &[String]) {
    let opts = tio_opts();
    let (matches, root, route) = tio_parseopts(opts, args);

    if matches.free.len() != 1 {
        panic!("Must specify firmware path only")
    }

    let firmware_data = std::fs::read(matches.free[0].clone()).unwrap();

    println!("Loaded {} bytes firmware", firmware_data.len());

    let port = tio::TioProxyPort::new(&root, None, None);
    let (tx, rx) = port.new_port(None, route, false, false).unwrap();

    tx.send(tio::Packet::rpc("dev.stop".to_string(), &vec![]))
        .unwrap();
    if let Err(_) = rpc_get_response(&rx) {
        // only if an error with the RPC happened.
        // TODO: some rpc errors should also fail, like timeouts or others.
        panic!("Failed to stop device");
    }

    let mut offset: usize = 0;

    while offset < firmware_data.len() {
        let chunk_end = if (offset + 288) > firmware_data.len() {
            firmware_data.len()
        } else {
            offset + 288
        };
        tx.send(tio::Packet::rpc(
            "dev.firmware.upload".to_string(),
            &firmware_data[offset..chunk_end],
        ))
        .unwrap();
        match rpc_get_reply(&rx, 0) {
            Ok(_reply) => {}
            _ => {
                panic!("upload failed");
            }
        };
        offset = chunk_end;
        let pct = 100.0 * (offset as f64) / (firmware_data.len() as f64);
        println!("Uploaded {:.1}%", pct);
    }

    tx.send(tio::Packet::rpc(
        "dev.firmware.upgrade".to_string(),
        &vec![],
    ))
    .unwrap();
    match rpc_get_reply(&rx, 0) {
        Ok(_) => {
            println!("Upgrade successful. Wait for sensor to reboot.");
        }
        _ => {
            panic!("upload failed");
        }
    };
}

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("help".to_string());
    }
    match args[1].as_str() {
        "rpc-list" => {
            list_rpcs(&args[2..]).unwrap();
        }
        "rpc" => {
            rpc(&args[2..]).unwrap();
        }
        "proxy" => {
            proxy(&args[2..]); //.unwrap();
        }
        "dump" => {
            dump(&args[2..]); //.unwrap();
        }
        "firmware-upgrade" => {
            firmware_upgrade(&args[2..]); //.unwrap();
        }
        _ => {
            // TODO: do usage right
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool proxy [-p port] [-v] [-d] [-t timefmt] (--auto | device-url)");
            println!(" tio-tool rpc [-r url] [-s sensor] [-t type] <rpc-name> [rpc-arg]");
            println!(" tio-tool rpc-list [-r url] [-s sensor]");
        }
    }
}

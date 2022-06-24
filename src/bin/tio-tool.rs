use tio::proto;
use tio::tio_addr;
use twinleaf::tio;

use std::env;
use std::net::TcpListener;

use getopts::Options;

fn random_stuff() {
    //let port = tio::TioPort::new(tio::serial::Port::new("/dev/ttyUSB0").unwrap()).unwrap();
    let port =
        tio::TioChannelPort::new(tio::tcp::Port::new(&tio_addr("127.0.0.1").unwrap()).unwrap())
            .unwrap();
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

    /*
       let mut listeners: Vec<mio::net::TcpListener> = Vec::new();

       let b4 = std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), tcp_port);
       let b6 = std::net::SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), tcp_port);

       if let Ok(listener) = mio::net::TcpListener::bind(b6) {
           listeners.push(listener);
           println!("BOUND v6");
       }
       if let Ok(listener) = mio::net::TcpListener::bind(b4) {
           listeners.push(listener);
           println!("BOUND v4");
       }

       println!("NLISTENERS: {}", listeners.len());

       use crossbeam::select;

       let mut port: Option<tio::TioPort> = None;
       let mut port_last_connected = std::time::Instant::now();

       let mut client: Option<tio::TioPort> = None;

       let mut poll = mio::Poll::new()?; // TODO: why mut??
       let mut events = mio::Events::with_capacity(1);
       let (sensor_rx_sender, sensor_rx) = crossbeam::channel::bounded::<Result<(u32, tio::Packet), tio::RecvError>>(32);
       let waker = mio::Waker::new(poll.registry(), mio::Token(1))?;
       let (client_rx_sender, client_rx) = crossbeam::channel::bounded::<Result<tio::Packet, tio::RecvError>>(32);
       for i in 0..listeners.len() {
       poll.registry()
           .register(&mut listeners[i], mio::Token(2+i), mio::Interest::READABLE)
               .unwrap();
       }

       loop {
           let timeout: Option<std::time::Duration> = None;
           if port.is_none() {
               // TODO: configurable timeout/no timeout
               if std::time::Instant::now() > (port_last_connected + std::time::Duration::from_secs(60)) {
                   println!("Sensor no longer connected, exiting.");
                   break;
               }
               let sensor_waker = mio::Waker::new(poll.registry(), mio::Token(0))?;
               let sensor_channel = sensor_rx_sender.clone();
               if let Ok(p) = tio::TioPort::from_url(&sensor_url, move |rxdata: Result<tio::Packet, tio::RecvError>| -> std::io::Result<()> {
                   use crossbeam::channel::TrySendError;
                   match sensor_channel.try_send(rxdata) {
                       Err(TrySendError::Full(_)) => {
                           // TODO: we are dropping packets. say somehting
                           Ok(())
                       }
                       Err(e) => {
                           if let TrySendError::Disconnected(_) = e {
                               Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
                           } else {
                               Err(std::io::Error::from(std::io::ErrorKind::Other))
                           }
                       }
                       Ok(_) => {
                           sensor_waker.wake();
                           Ok(())
                       }
                   }
               }) { port = Some(p);
                   println!("Connected to sensor");
               } else {
                   println!("Connect to sensor failed.");
               }
           }

           let timeout = match port {
               Some(_) => { None }
               None => {Some(std::time::Duration::from_secs(1))}
           };

           poll.poll(&mut events, timeout).unwrap();

           for event in events.iter() {
               match event.token() {
                   mio::Token(0) => {
                       // event on the sensor
                       loop {
                           for pkt in queue_rx.try_iter() {
                               match pkt {
                               Ok(pkt) => {
                                   if client.is_some() {
                                       client.as_ref().unwrap().send(pkt);
                                       // TODO: send errors
                                       /*
                                       if let Err(_) = cl.send(pkt) {
                                           // TODO:Does this drop/disconnect client??
                                           println!("Disconnecting client!!");
                                           client = None;
                                           port_last_connected = std::time::Instant::now();
                                       }
                                       */
                                   }
                               }
                               Err(tio::RecvError::NotReady) => {
                                   break;
                               }
                               Err(e) => {
                                   // Sensor error
                                   println!("Disconnecting sensor!!");
                                   port = None;
                                   break;
                               }
                               }
                           };
                       }
                   }
                   mio::Token(1) => {
                       // event on the client
                       for pkt in client_rx.try_iter() {
                           match pkt {
                               Ok(pkt) => {
                                   port.as_ref().unwrap().send(pkt);
                               }
                               Err(_) => {
                                   panic!("TODO");
                                   //break 'ioloop;
                               }
                           }
                       }
                   }
                   mio::Token(x) => {
                       if (x-2) >= listeners.len() {
                           panic!("Unexpected token {}", x);
                       }
                       let listener = &listeners[x-2];
                       match listener.accept() {
                           Ok((stream,addr)) => {
                               println!("Connection from client: {:?}", addr);
                           }
                           _ => {}
                       }
                   }
               }
           }

       }
    */
    use crossbeam::select;

    let port = tio::TioChannelPort::from_url(&sensor_url)?;
    //    let listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port))?;
    let listener = TcpListener::bind(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        tcp_port,
    ))?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nonblocking(true);
        let client = tio::tcp::Port::from_stream(mio::net::TcpStream::from_std(stream));
        let client = match tio::TioChannelPort::new(client?) {
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

    let port = tio::TioChannelPort::from_url(&root)?;
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

fn testproxy() {
    let listener = TcpListener::bind(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        7855,
    ))
    .unwrap();

    let url = {
        let devices = enum_devices(false);
        if devices.len() != 1 {
            panic!("TODO");
        }
        devices[0].url.clone()
    };

    let port = tio::TioProxyPort::new(&url);

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            _ => {
                continue;
            }
        };
        stream.set_nonblocking(true);
        let client = tio::tcp::Port::from_stream(mio::net::TcpStream::from_std(stream));
        let client = match tio::TioChannelPort::new(client.unwrap()) {
            Ok(client_port) => client_port,
            _ => continue,
        };

        let (sender, receiver) = port.new_proxy();
        let thd = std::thread::spawn(move || {
            use crossbeam::select;
            loop {
                select! {
                    recv(receiver) -> res => {
                        let pkt = res.unwrap(); // port failing will close program
                        //if verbose {
                        println!("{}", tio::log_msg("port->client", &pkt));
                        //}
                        client.send(pkt);
                    }
                    recv(client.rx) -> res => {
                        match res {
                            Ok(Ok(pkt)) => {
                                //if verbose {
                                println!("{}", tio::log_msg("client->port", &pkt));
                                //}
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

fn testthread(id: u32, rx: crossbeam::channel::Receiver<tio::Packet>) {
    loop {
        let pkt = match rx.recv() {
            Ok(pkt) => pkt,
            Err(e) => {
                println!("Exiting due to error: {:?}", e);
                break;
            }
        };
        if let proto::Payload::StreamData(sample) = pkt.payload {
            println!(
                "THD{}: Stream {}: sample {} {:?}",
                id, sample.stream_id, sample.sample_n, sample.data
            );
        }
    }
}

fn testproxyport() {
    let port = tio::TioProxyPort::new("/dev/ttyUSB0");
    //let port = tio::TioProxyPort::new("tcp://localhost");

    let (send1, recv1) = port.new_proxy();
    let thd1 = std::thread::spawn(move || {
        testthread(1, recv1);
    });
    let (send2, recv2) = port.new_proxy();
    let thd2 = std::thread::spawn(move || {
        testthread(2, recv2);
    });
    loop {}
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
        "proxy2" => {
            testproxy();
        }
        "test2" => {
            testproxyport();
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

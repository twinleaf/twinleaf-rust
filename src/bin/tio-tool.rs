use tio::proto;
use tio::tio_addr;
use twinleaf::tio;

use std::env;
use std::net::TcpListener;

fn random_stuff() {
    let port = tio::TioPort::new(tio::serial::Port::new("/dev/ttyUSB0").unwrap()).unwrap();
    //let port = tio::TioPort::new(tio::tcp::Port::new(&tio_addr("localhost").unwrap()).unwrap()).unwrap();
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

// this is wrong in so many ways
fn seqproxy(wasteful: bool) -> std::io::Result<()> {
    use crossbeam::select;

    let port = tio::TioPort::new(tio::serial::Port::new("/dev/ttyUSB0").unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:7855")?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nonblocking(true);
        let client = tio::tcp::Port::from_stream(mio::net::TcpStream::from_std(stream));
        let client = match tio::TioPort::new(client?) {
            Ok(port) => port,
            _ => continue,
        };

        if wasteful {
            use crossbeam::channel::TryRecvError;
            loop {
                match port.rx.try_recv() {
                    Ok(Ok(pkt)) => {
                        client.send(pkt);
                    }
                    Err(TryRecvError::Empty) => {}
                    _ => {
                        panic!("Sensor error");
                    }
                }
                match client.rx.try_recv() {
                    Ok(Ok(pkt)) => {
                        port.send(pkt);
                    }
                    Err(TryRecvError::Empty) => {}
                    _ => {
                        // client failing will listen for the next client
                        println!("Client exiting");
                        break;
                    }
                }
            }
        } else {
            loop {
                select! {
                    recv(port.rx) -> res => {
                        let pkt = res.unwrap().unwrap(); // port failing will close program
                        client.send(pkt);
                    }
                    recv(client.rx) -> res => {
                        match res {
                            Ok(Ok(pkt)) => {
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
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    match args[1].as_str() {
        "proxy" => {
            seqproxy(false).unwrap();
        }
        "pproxy" => {
            seqproxy(true).unwrap();
        }
        "test" => {
            random_stuff();
        }
        unknown => {
            panic!("unknown command: {}", unknown);
        }
    }
}

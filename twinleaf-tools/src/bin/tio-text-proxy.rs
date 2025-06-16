use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::{env, thread};
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

fn broadcast_to_client(mut stream: TcpStream, port: tio::proxy::Port) {
    use twinleaf::data::Device;
    let mut device = Device::new(port);
    let peer_addr = stream.peer_addr().unwrap();
    println!("Connection from: {}", peer_addr);

    loop {
        let sample = device.next();
        if let Err(_) = writeln!(stream, "{}", sample) {
            break;
        }
    }
    println!("Disconnected: {}", peer_addr);
}

fn main() {
    let opts = tio_opts();
    let args: Vec<String> = env::args().collect();
    let (_matches, root, route) = tio_parseopts(&opts, &args);

    let proxy = tio::proxy::Interface::new(&root);

    let listener = TcpListener::bind("0.0.0.0:7800").unwrap();
    for connection in listener.incoming() {
        if let Ok(stream) = connection {
            let device = proxy.device_full(route.clone()).unwrap();
            thread::spawn(move || broadcast_to_client(stream, device));
        }
    }
}

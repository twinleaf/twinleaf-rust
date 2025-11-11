use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::{env, thread};
use twinleaf::device::Device;
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

fn format_nmea_sentence(talker_id: &str, sentence_type: &str, fields: &[String]) -> String {
    let mut sentence = format!("${}{}", talker_id, sentence_type);
    for field in fields {
        sentence.push(',');
        sentence.push_str(field);
    }

    // Calculate checksum
    let checksum = sentence[1..].bytes().fold(0u8, |acc, b| acc ^ b);
    format!("{}*{:02X}\r\n", sentence, checksum)
}

fn broadcast_to_client(mut stream: TcpStream, port: tio::proxy::Port) {
    let mut device = Device::new(port);
    let peer_addr = stream.peer_addr().unwrap();
    println!("Connection from: {}", peer_addr);

    loop {
        let sample = match device.next() {
            Ok(sample) => sample,
            Err(_) => {
                eprintln!("Failed to parse sample");
                break;
            }
        };

        // Only process samples from stream ID 1
        if sample.stream.stream_id != 1 {
            continue;
        }

        // Convert timestamp to NMEA format (HHMMSS.SS)
        let timestamp = sample.timestamp_end();
        let hours = (timestamp / 3600.0) as u32;
        let minutes = ((timestamp % 3600.0) / 60.0) as u32;
        let seconds = timestamp % 60.0;
        let time_str = format!("{:02}{:02}{:05.2}", hours, minutes, seconds);

        // Format data fields
        let mut fields = vec![time_str];
        for col in &sample.columns {
            let value = match col.value {
                twinleaf::data::ColumnData::Int(x) => format!("{}", x),
                twinleaf::data::ColumnData::UInt(x) => format!("{}", x),
                twinleaf::data::ColumnData::Float(x) => format!("{:.2}", x),
                twinleaf::data::ColumnData::Unknown => "?".to_string(),
            };
            fields.push(value);
        }

        // Create NMEA sentence
        let nmea = format_nmea_sentence("TL", "MAG", &fields);

        if let Err(_) = write!(stream, "{}", nmea) {
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

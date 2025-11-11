use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
use twinleaf::device::Device;
use twinleaf::tio;
use twinleaf_tools::TioOpts;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "tio-text-proxy",
    version,
    about = "Bridge Twinleaf sensor data to NMEA TCP stream"
)]
struct Cli {
    #[command(flatten)]
    tio: TioOpts,

    #[arg(
        short = 'p',
        long = "port",
        default_value = "7800",
        help = "TCP port to listen on"
    )]
    tcp_port: u16,
}

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
    let cli = Cli::parse();

    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let route = cli.tio.parse_route();

    let bind_addr = format!("0.0.0.0:{}", cli.tcp_port);
    let listener = TcpListener::bind(&bind_addr)
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", bind_addr, e));

    println!("Listening on {}", bind_addr);

    for connection in listener.incoming() {
        if let Ok(stream) = connection {
            let device = proxy.device_full(route.clone()).unwrap();
            thread::spawn(move || broadcast_to_client(stream, device));
        }
    }
}

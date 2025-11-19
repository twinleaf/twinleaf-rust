use clap::{Parser, Subcommand};
use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::DeviceDataParser;
use twinleaf::device;
use twinleaf::device::{Device, DeviceTree};
use twinleaf::tio;
use twinleaf_tools::TioOpts;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[command(
    name = "tio-tool",
    version,
    about = "Twinleaf sensor management and data logging tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List available RPCs on the device
    RpcList {
        #[command(flatten)]
        tio: TioOpts,
    },
    /// Execute an RPC on the device
    Rpc {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to execute
        rpc_name: String,

        /// RPC argument value
        #[arg(
            allow_negative_numbers = true,
            value_name = "ARG",
            help_heading = "RPC Arguments"
        )]
        rpc_arg: Option<String>,

        /// RPC request type (one of: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, string)
        #[arg(short = 't', long = "req-type", help_heading = "Type Options")]
        req_type: Option<String>,

        /// RPC reply type (one of: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, string)
        #[arg(short = 'T', long = "rep-type", help_heading = "Type Options")]
        rep_type: Option<String>,

        /// Enable debug output
        #[arg(short = 'd', long)]
        debug: bool,
    },

    /// Dump RPC data from the device
    RpcDump {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to dump
        rpc_name: String,

        /// Trigger and dump a capture buffer
        #[arg(long)]
        capture: bool,
    },

    /// Dump raw packets from the device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// Dump depth (default: dump everything)
        #[arg(short = 'd', long = "depth")]
        depth: Option<usize>,
    },

    /// Log samples to a file, by default including metadata. 
    /// Use --raw to skip metadata request and dump raw packets.
    Log {
        #[command(flatten)]
        tio: TioOpts,

        /// Path of file where to log the data
        #[arg(short = 'f', default_value_t = default_log_path())]
        file: String,

        /// Unbuffered output (flush every packet)
        #[arg(short = 'u')]
        unbuffered: bool,

        /// Raw mode: Dumps all packets read
        #[arg(long)]
        raw: bool,

        /// Packet depth (only used in --raw mode)
        #[arg(short = 'd', long = "depth")]
        depth: Option<usize>,
    },

    /// Log metadata to a file
    LogMetadata {
        #[command(flatten)]
        tio: TioOpts,

        /// Path of file where to store the metadata
        #[arg(short = 'f', default_value = "meta.tio")]
        file: String,
    },

    /// Dump logged packets from file(s)
    LogDump {
        /// Log file paths
        files: Vec<String>,
    },

    /// Dump logged data from file(s)
    LogDataDump {
        /// Log file paths
        files: Vec<String>,
    },

    /// Convert logged data to CSV
    LogCsv {
        /// Stream ID
        stream_id: u8,

        /// Log file paths (first file after stream_id)
        files: Vec<String>,

        /// Sensor path in the sensor tree (default /)
        #[arg(short = 's')]
        sensor: Option<String>,

        /// Metadata file (if separate)
        #[arg(short = 'm')]
        metadata: Option<String>,

        /// Output file prefix
        #[arg(short = 'o')]
        output: Option<String>,
    },

    /// Upgrade device firmware
    FirmwareUpgrade {
        #[command(flatten)]
        tio: TioOpts,

        /// Firmware image path
        firmware_path: String,
    },

    /// Dump data samples from the device
    DataDump {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump data samples from all devices in tree
    DataDumpAll {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump device metadata
    MetaDump {
        #[command(flatten)]
        tio: TioOpts,
    },
}

fn default_log_path() -> String {
    chrono::Local::now()
        .format("log.%Y%m%d-%H%M%S.tio")
        .to_string()
}

fn list_rpcs(tio: &TioOpts) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let device = proxy.device_rpc(route).unwrap();

    let specs = device::util::load_rpc_specs(&device).map_err(|e| {
        eprintln!("Failed to load RPC specs: {:?}", e);
    })?;

    for spec in specs {
        println!(
            "{} {}({})",
            spec.perm_str(),
            spec.full_name,
            spec.type_str()
        );
    }

    Ok(())
}

fn get_rpctype(name: &String, device: &proxy::Port) -> String {
    if let Ok(meta) = device.rpc("rpc.info", name) {
        let spec = device::util::parse_rpc_spec(meta, name.clone());
        spec.type_str()
    } else {
        "".to_string()
    }
}

fn rpc(
    tio: &TioOpts,
    rpc_name: String,
    rpc_arg: Option<String>,
    req_type: Option<String>,
    rep_type: Option<String>,
    debug: bool,
) -> Result<(), ()> {
    let (status_send, proxy_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let proxy = proxy::Interface::new_proxy(&tio.root, None, Some(status_send));
    let route = tio.parse_route();
    let device = proxy.device_rpc(route).unwrap();

    let req_type = if let Some(req_type) = req_type {
        Some(req_type)
    } else {
        if rpc_arg.is_some() {
            let t = get_rpctype(&rpc_name, &device);
            Some(if t == "" {
                println!("Unknown RPC arg type, assuming 'string'. Use -t/--req-type to override.");
                "string".to_string()
            } else {
                t
            })
        } else {
            None
        }
    };

    let reply = match device.raw_rpc(
        &rpc_name,
        &if rpc_arg.is_none() {
            vec![]
        } else {
            let s = rpc_arg.unwrap();
            match &req_type.as_ref().unwrap()[..] {
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
    ) {
        Ok(rep) => rep,
        Err(err) => {
            drop(proxy);
            if debug {
                println!("RPC failed: {:?}", err);
                for s in proxy_status.try_iter() {
                    println!("{:?}", s);
                }
            } else {
                if let proxy::RpcError::ExecError(rpc_err) = err {
                    println!("RPC failed: {:?}", rpc_err.error);
                } else {
                    println!("RPC failed, run with `-d` for more details.");
                }
            }
            return Err(());
        }
    };

    if reply.len() != 0 {
        let rep_type = if let Some(rep_type) = rep_type {
            Some(rep_type)
        } else {
            if let None = req_type {
                let t = get_rpctype(&rpc_name, &device);
                Some(if t == "" {
                    println!(
                        "Unknown RPC ret type, assuming 'string'. Use -T/--ret-type to override."
                    );
                    "string".to_string()
                } else {
                    t
                })
            } else {
                req_type
            }
        };
        let reply_str = match &rep_type.as_ref().unwrap()[..] {
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
    drop(proxy);
    for s in proxy_status.iter() {
        if debug {
            println!("{:?}", s);
        }
    }
    Ok(())
}

fn rpc_dump(tio: &TioOpts, rpc_name: String, is_capture: bool) -> Result<(), ()> {
    let rpc_name = if is_capture {
        rpc_name.clone() + ".block"
    } else {
        rpc_name.clone()
    };

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let device = proxy.device_rpc(route).unwrap();

    if is_capture {
        let trigger_rpc_name = rpc_name[..rpc_name.len() - 6].to_string() + ".trigger";
        device.action(&trigger_rpc_name).unwrap();
    }

    let mut full_reply = vec![];

    for i in 0u16..=65535u16 {
        match device.raw_rpc(&rpc_name, &i.to_le_bytes().to_vec()) {
            Ok(mut rep) => full_reply.append(&mut rep),
            Err(proxy::RpcError::ExecError(err)) => {
                if let tio::proto::RpcErrorCode::InvalidArgs = err.error {
                    break;
                } else {
                    panic!("RPC error");
                }
            }
            _ => {
                panic!("RPC error")
            }
        }
    }

    if let Ok(s) = std::str::from_utf8(&full_reply) {
        println!("{}", s);
    } else {
        std::io::stdout().write(&full_reply).or(Err(()))?;
    }
    Ok(())
}

fn dump(tio: &TioOpts, depth: Option<usize>) -> Result<(), ()> {
    let depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let port = proxy
        .new_port(None, route, depth, true, true)
        .map_err(|e| {
            eprintln!("Failed to initialize proxy port: {:?}", e);
        })?;

    for pkt in port.iter() {
        println!("{:?}", pkt);
    }
    Ok(())
}

fn meta_dump(tio: &TioOpts) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    let mut device = Device::open(&proxy, route).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    let meta = device.get_metadata().map_err(|e| {
        eprintln!("Failed to get metadata: {:?}", e);
    })?;

    println!("{:?}", meta.device);
    for (_id, stream) in meta.streams {
        println!("{:?}", stream.stream);
        println!("{:?}", stream.segment);
        for col in stream.columns {
            println!("{:?}", col);
        }
    }
    Ok(())
}

fn print_sample(sample: &twinleaf::data::Sample, route: Option<&DeviceRoute>) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };
    if sample.meta_changed {
        println!("# {}DEVICE {:?}", route_str, sample.device);
        println!("# {}STREAM {:?}", route_str, sample.stream);
        for col in &sample.columns {
            println!("# {}COLUMN {:?}", route_str, col.desc);
        }
    }
    if sample.segment_changed {
        println!("# {}SEGMENT {:?}", route_str, sample.segment);
    }
    println!("{}{}", route_str, sample);
}

fn data_dump(tio: &TioOpts) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    let mut device = Device::open(&proxy, route).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    loop {
        match device.next() {
            Ok(sample) => print_sample(&sample, None),
            Err(e) => {
                eprintln!("\nDevice error: {:?}. Exiting.", e);
                break;
            }
        }
    }
    Ok(())
}

fn data_dump_all(tio: &TioOpts) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    let mut devs = twinleaf::device::DeviceTree::open(&proxy, route).map_err(|e| {
        eprintln!("open failed: {:?}", e);
    })?;

    loop {
        match devs.drain() {
            Ok(batch) => {
                for (s, r) in batch {
                    print_sample(&s, Some(&r));
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

fn log(
    tio: &TioOpts,
    file: String,
    unbuffered: bool,
    raw: bool,
    depth: Option<usize>,
) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    if raw {
        let depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);
        let port = proxy
            .new_port(None, route, depth, true, true)
            .map_err(|e| {
                eprintln!("Failed to initialize proxy port: {:?}", e);
            })?;

        let mut file = File::create(file).unwrap();
        println!("Logging raw packets...");

        for pkt in port.iter() {
            let raw = pkt.serialize().unwrap();
            file.write_all(&raw).unwrap();
            if unbuffered {
                file.flush().unwrap();
            }
        }
        return Ok(());
    }

    let mut file = File::create(file).map_err(|e| {
        eprintln!("create failed: {e:?}");
    })?;

    let mut devs = DeviceTree::open(&proxy, route.clone()).map_err(|e| {
        eprintln!("open failed: {:?}", e);
    })?;

    let mut seen_routes: HashSet<DeviceRoute> = HashSet::new();

    let write_packet = |pkt: tio::Packet, f: &mut File| -> std::io::Result<()> {
        f.write_all(&pkt.serialize().unwrap())
    };

    match devs.get_metadata(route.clone()) {
        Ok(meta) => {
            seen_routes.insert(route.clone());
            
            let _ = write_packet(meta.device.make_update_with_route(route.clone()), &mut file);
            for (_id, stream) in meta.streams {
                let _ = write_packet(stream.stream.make_update_with_route(route.clone()), &mut file);
                let _ = write_packet(stream.segment.make_update_with_route(route.clone()), &mut file);
                for col in stream.columns {
                    let _ = write_packet(col.make_update_with_route(route.clone()), &mut file);
                }
            }
            if unbuffered { let _ = file.flush(); }
        }
        Err(e) => {
            eprintln!("Note: Initial metadata fetch skipped: {:?}", e);
        }
    }

    println!("Logging data...");

    loop {
        match devs.drain() {
            Ok(batch) => {
                for (sample, sample_route) in batch {
                    let is_new_device = seen_routes.insert(sample_route.clone());
                    let force_header = is_new_device; 

                    if sample.meta_changed || force_header {
                        let _ = write_packet(sample.device.make_update_with_route(sample_route.clone()), &mut file);
                        let _ = write_packet(sample.stream.make_update_with_route(sample_route.clone()), &mut file);
                        let _ = write_packet(sample.segment.make_update_with_route(sample_route.clone()), &mut file);
                        for col in sample.columns {
                            let _ = write_packet(col.desc.make_update_with_route(sample_route.clone()), &mut file);
                        }
                    } else if sample.segment_changed {
                        let _ = write_packet(sample.segment.make_update_with_route(sample_route.clone()), &mut file);
                    }

                    let data_pkt = tio::Packet {
                        payload: tio::proto::Payload::StreamData(sample.source),
                        routing: sample_route.clone(),
                        ttl: 0,
                    };
                    let _ = write_packet(data_pkt, &mut file);
                }
            }
            Err(e) => {
                eprintln!("Device error: {e:?}");
                break;
            }
        }

        if unbuffered {
            if let Err(e) = file.flush() {
                eprintln!("flush error: {e:?}");
                break;
            }
        }
    }
    Ok(())
}

fn log_metadata(tio: &TioOpts, file: String) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    let mut device = Device::open(&proxy, route).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    let meta = device.get_metadata().map_err(|e| {
        eprintln!("Failed to get metadata: {:?}", e);
    })?;

    let mut file = File::create(file).unwrap();

    file.write_all(&meta.device.make_update().serialize().unwrap())
        .unwrap();
    for (_id, stream) in meta.streams {
        file.write_all(&stream.stream.make_update().serialize().unwrap())
            .unwrap();
        file.write_all(&stream.segment.make_update().serialize().unwrap())
            .unwrap();
        for col in stream.columns {
            file.write_all(&col.make_update().serialize().unwrap())
                .unwrap();
        }
    }
    Ok(())
}

fn log_dump(files: Vec<String>) -> Result<(), ()> {
    for path in files {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            println!("{:?}", pkt);
        }
    }
    Ok(())
}

fn log_data_dump(files: Vec<String>) -> Result<(), ()> {
    use twinleaf::data::DeviceDataParser;
    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;

    for path in files {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];

            let parser = parsers.entry(pkt.routing.clone()).or_insert_with(|| DeviceDataParser::new(ignore_session));

            for sample in parser.process_packet(&pkt) {
                print_sample(&sample, Some(&pkt.routing));
            }
        }
    }
    Ok(())
}

fn log_csv(
    stream_id: u8,
    files: Vec<String>,
    sensor: Option<String>,
    metadata: Option<String>,
    output: Option<String>,
) -> Result<(), ()> {
    if files.is_empty() {
        eprintln!("Invalid invocation: missing log file");
        return Err(());
    }

    let target_route = if let Some(path) = sensor {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };

    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1 || metadata.is_some();

    if let Some(path) = metadata {
        let mut meta: &[u8] = &std::fs::read(path).unwrap();
        while meta.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(meta).unwrap();
            meta = &meta[len..];

            let parser = parsers.entry(pkt.routing.clone()).or_insert_with(|| DeviceDataParser::new(ignore_session));
            for _ in parser.process_packet(&pkt) {}
        }
    }

    let output_path = format!(
        "{}.{}.csv",
        output.unwrap_or_else(|| files[0].clone()),
        stream_id
    );

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path)
        .or(Err(()))?;

    let mut header_written: bool = false;

    for path in &files {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];

            let parser = parsers.entry(pkt.routing.clone()).or_insert_with(|| DeviceDataParser::new(ignore_session));
            let samples = parser.process_packet(&pkt);

            if pkt.routing != target_route {
                continue;
            }

            for sample in samples {
                if sample.stream.stream_id != stream_id {
                    continue;
                }

                if !header_written {
                    let mut headers: Vec<String> = vec!["time".to_string()];
                    headers.extend(sample.columns.iter().map(|col| col.desc.name.clone()));

                    writeln!(file, "{}", headers.join(",")).or(Err(()))?;
                    header_written = true;
                }

                let mut values: Vec<String> = Vec::new();
                values.push(format!("{:.6}", sample.timestamp_end()));

                values.extend(sample.columns.iter().map(|col| col.value.to_string()));

                writeln!(file, "{}", values.join(",")).or(Err(()))?;
            }
        }
    }
    Ok(())
}

fn firmware_upgrade(tio: &TioOpts, firmware_path: String) -> Result<(), ()> {
    let firmware_data = std::fs::read(firmware_path).unwrap();

    println!("Loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let device = proxy.device_rpc(route).unwrap();

    if let Err(_) = device.action("dev.stop") {
        // TODO: should ignore some errors, such as method not existing or if already stopped.
        //panic!("Failed to stop device");
        println!("Failed to stop device");
    }

    let mut next_send_chunk: u16 = 0;
    let mut next_ack_chunk: u16 = 0;
    let mut more_to_send = true;
    const MAX_CHUNKS_IN_FLIGHT: u16 = 2;

    while more_to_send || (next_ack_chunk != next_send_chunk) {
        if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            let offset = usize::from(next_send_chunk) * 288;
            let chunk_end = if (offset + 288) > firmware_data.len() {
                firmware_data.len()
            } else {
                offset + 288
            };

            if let Err(_) = device.send(util::PacketBuilder::make_rpc_request(
                "dev.firmware.upload",
                &firmware_data[offset..chunk_end],
                next_send_chunk,
                DeviceRoute::root(),
            )) {
                panic!("Upload failed");
            }
            next_send_chunk += 1;
            more_to_send = chunk_end < firmware_data.len();
        }

        let pkt = if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            match device.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => continue,
                Err(_) => panic!("Upload failed"),
            }
        } else {
            device.recv().expect("Upload failed")
        };

        match pkt.payload {
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id != next_ack_chunk {
                    panic!("Upload failed");
                }

                let pct = 100.0 * ((next_ack_chunk as f64) * 288.0) / (firmware_data.len() as f64);
                println!("Uploaded {:.1}%", pct);
                next_ack_chunk += 1;
            }
            tio::proto::Payload::RpcError(err) => {
                //if let RpcError::InvalidArgs = err.error {
                // TODO: we could handle this condition, likely caused by
                // a packet dropped
                //}
                panic!("Upload failed: {:?}", err)
            }
            _ => continue,
        }
    }

    // The loop above conceptually does this, but allowing multiple
    // RPCs in flight.
    /*
    let mut offset: usize = 0;
    while offset < firmware_data.len() {
        let chunk_end = if (offset + 288) > firmware_data.len() {
            firmware_data.len()
        } else {
            offset + 288
        };
        match device.raw_rpc("dev.firmware.upload", &firmware_data[offset..chunk_end]) {
            Ok(_reply) => {}
            _ => {
                panic!("upload failed");
            }
        };
        offset = chunk_end;
        let pct = 100.0 * (offset as f64) / (firmware_data.len() as f64);
        println!("Uploaded {:.1}%", pct);
    }
    */

    if let Err(_) = device.action("dev.firmware.upgrade") {
        panic!("upgrade failed");
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::RpcList { tio } => list_rpcs(&tio),
        Commands::Rpc {
            tio,
            rpc_name,
            rpc_arg,
            req_type,
            rep_type,
            debug,
        } => rpc(&tio, rpc_name, rpc_arg, req_type, rep_type, debug),
        Commands::RpcDump {
            tio,
            rpc_name,
            capture,
        } => rpc_dump(&tio, rpc_name, capture),
        Commands::Dump { tio, depth } => dump(&tio, depth),
        Commands::Log {
            tio,
            file,
            unbuffered,
            raw,
            depth,
        } => log(&tio, file, unbuffered, raw, depth),
        Commands::LogMetadata { tio, file } => log_metadata(&tio, file),
        Commands::LogDump { files } => log_dump(files),
        Commands::LogDataDump { files } => log_data_dump(files),
        Commands::LogCsv {
            stream_id,
            files,
            sensor,
            metadata,
            output,
        } => log_csv(stream_id, files, sensor, metadata, output),
        Commands::FirmwareUpgrade { tio, firmware_path } => firmware_upgrade(&tio, firmware_path),
        Commands::DataDump { tio } => data_dump(&tio),
        Commands::DataDumpAll { tio } => data_dump_all(&tio),
        Commands::MetaDump { tio } => meta_dump(&tio),
    };

    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        eprintln!("FAILED");
        ExitCode::FAILURE
    }
}

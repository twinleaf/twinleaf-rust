use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::DeviceDataParser;
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::process::ExitCode;

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

fn list_rpcs(args: &[String]) -> Result<(), ()> {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(&opts, args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_rpc(route).unwrap();

    let nrpcs: u16 = device.get("rpc.listinfo").unwrap();

    for rpc_id in 0u16..nrpcs {
        let (meta, name): (u16, String) = device.rpc("rpc.listinfo", rpc_id).unwrap();
        let meta = RpcMeta::parse(meta);
        println!("{} {}({})", meta.perm_str(), name, meta.type_str());
    }

    Ok(())
}

fn get_rpctype(name: &String, device: &proxy::Port) -> String {
    if let Ok(reply) = device.rpc("rpc.info", name) {
        RpcMeta::parse(reply).arg_type
    } else {
        "".to_string()
    }
}

fn rpc(args: &[String]) -> Result<(), ()> {
    let mut opts = tio_opts();
    opts.optopt(
        "t",
        "req-type",
        "RPC request type (one of u8/u16/u32/u64 i8/i16/i32/i64 f32/f64 string). ",
        "type",
    );
    opts.optopt(
        "T",
        "rep-type",
        "RPC reply type (one of u8/u16/u32/u64 i8/i16/i32/i64 f32/f64 string). ",
        "type",
    );
    opts.optflag("d", "", "Debug printouts.");
    let (matches, root, route) = tio_parseopts(&opts, args);

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

    let debug = matches.opt_present("d");

    let (status_send, proxy_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let proxy = proxy::Interface::new_proxy(&root, None, Some(status_send));
    let device = proxy.device_rpc(route).unwrap();
    let req_type = if let Some(req_type) = matches.opt_str("req-type") {
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
        let rep_type = if let Some(rep_type) = matches.opt_str("rep-type") {
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

fn rpc_dump(args: &[String]) -> Result<(), ()> {
    let mut opts = tio_opts();
    opts.optflag("", "capture", "Trigger and dump a capture buffer");
    let (matches, root, route) = tio_parseopts(&opts, args);
    let is_capture = matches.opt_present("capture");

    let rpc_name = if matches.free.len() != 1 {
        panic!("must specify rpc name")
    } else {
        if is_capture {
            matches.free[0].clone() + ".block"
        } else {
            matches.free[0].clone()
        }
    };

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_rpc(route).unwrap();

    if is_capture {
        let trigger_rpc_name = matches.free[0].clone() + ".trigger";
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

fn dump(args: &[String]) -> Result<(), ()> {
    let mut opts = tio_opts();
    opts.optopt(
        "d",
        "depth",
        "Dump depth (default: dump everything)",
        "max-depth",
    );
    let (matches, root, route) = tio_parseopts(&opts, args);
    let depth = matches
        .opt_str("d")
        .unwrap_or(format!("{}", tio::proto::TIO_PACKET_MAX_ROUTING_SIZE))
        .parse::<usize>()
        .map_err(|e| {
            eprintln!("Failed to parse depth: {:?}", e);
        })?;

    let proxy = proxy::Interface::new(&root);
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

fn meta_dump(args: &[String]) -> Result<(), ()> {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(&opts, args);

    let proxy = proxy::Interface::new(&root);
    let mut device = twinleaf::Device::open(&proxy, route).map_err(|e| {
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

fn data_dump(args: &[String]) -> Result<(), ()> {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(&opts, args);

    let proxy = proxy::Interface::new(&root);
    let mut device = twinleaf::Device::open(&proxy, route).map_err(|e| {
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

fn all_data_dump(args: &[String]) -> Result<(), ()> {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(&opts, args);
    let proxy = proxy::Interface::new(&root);

    let mut devs = twinleaf::device::DeviceTree::open(proxy, route).map_err(|_| {})?;
    while let Ok(samples) = devs.drain(true) {
        for (sample, route) in samples {
            print_sample(&sample, Some(&route));
        }
    }
    Ok(())
}

fn log(args: &[String]) -> Result<(), ()> {
    let output_path = chrono::Local::now().format("log.%Y%m%d-%H%M%S.tio");
    let mut opts = tio_opts();
    opts.optopt(
        "f",
        "",
        &format!(
            "path of file where to log the data (default {})",
            output_path
        ),
        "path",
    );
    opts.optflag("u", "", "unbuffered output");
    opts.optopt(
        "d",
        "depth",
        "Dump depth (default: dump everything)",
        "max-depth",
    );
    let (matches, root, route) = tio_parseopts(&opts, args);
    let depth = matches
        .opt_str("d")
        .unwrap_or(format!("{}", tio::proto::TIO_PACKET_MAX_ROUTING_SIZE))
        .parse::<usize>()
        .map_err(|e| {
            eprintln!("Failed to parse depth: {:?}", e);
        })?;
    if matches.free.len() != 0 {
        print!("{}", opts.usage("Unexpected argument"));
        return Err(());
    }

    let output_path = if let Some(path) = matches.opt_str("f") {
        path
    } else {
        output_path.to_string()
    };

    let proxy = proxy::Interface::new(&root);
    let port = proxy
        .new_port(None, route, depth, true, true)
        .map_err(|e| {
            eprintln!("Failed to initialize proxy port: {:?}", e);
        })?;

    let mut file = File::create(output_path).unwrap();
    let sync = matches.opt_present("u");

    for pkt in port.iter() {
        let raw = pkt.serialize().unwrap();
        file.write_all(&raw).unwrap();
        if sync {
            file.flush().unwrap();
        }
    }
    Ok(())
}

fn log_data(args: &[String]) -> Result<(), ()> {
    let output_path = chrono::Local::now().format("log.%Y%m%d-%H%M%S.tio");
    let mut opts = tio_opts();
    opts.optopt(
        "f",
        "",
        &format!(
            "path of file where to log the data (default {})",
            output_path
        ),
        "path",
    );
    opts.optflag("u", "", "unbuffered output");
    let (matches, root, route) = tio_parseopts(&opts, args);
    if matches.free.len() != 0 {
        print!("{}", opts.usage("Unexpected argument"));
        return Err(());
    }

    let output_path = if let Some(path) = matches.opt_str("f") {
        path
    } else {
        output_path.to_string()
    };
    let sync = matches.opt_present("u");

    let proxy = proxy::Interface::new(&root);
    let mut file = File::create(output_path).unwrap();

    let mut devs = twinleaf::device::DeviceTree::open(proxy, route).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    while let Ok(samples) = devs.drain(true) {
        for (sample, route) in samples {
            if sample.meta_changed {
                file.write_all(
                    &sample
                        .device
                        .make_update2(route.clone())
                        .serialize()
                        .unwrap(),
                )
                .unwrap();
                file.write_all(
                    &sample
                        .stream
                        .make_update2(route.clone())
                        .serialize()
                        .unwrap(),
                )
                .unwrap();
                file.write_all(
                    &sample
                        .segment
                        .make_update2(route.clone())
                        .serialize()
                        .unwrap(),
                )
                .unwrap();
                for col in sample.columns {
                    file.write_all(&col.desc.make_update2(route.clone()).serialize().unwrap())
                        .unwrap();
                }
            } else if sample.segment_changed {
                file.write_all(
                    &sample
                        .segment
                        .make_update2(route.clone())
                        .serialize()
                        .unwrap(),
                )
                .unwrap();
            }
            file.write_all(
                &tio::Packet {
                    payload: tio::proto::Payload::StreamData(sample.source),
                    routing: route.clone(),
                    ttl: 0,
                }
                .serialize()
                .unwrap(),
            )
            .unwrap();
        }
        if sync {
            file.flush().unwrap();
        }
    }
    Ok(())
}

fn log_metadata(args: &[String]) -> Result<(), ()> {
    let mut opts = tio_opts();
    opts.optopt(
        "f",
        "",
        "path of file where to store the metadata (defaults meta.tio)",
        "path",
    );
    let (matches, root, route) = tio_parseopts(&opts, args);
    if matches.free.len() != 0 {
        print!("{}", opts.usage("Unexpected argument"));
        return Err(());
    }

    let proxy = proxy::Interface::new(&root);
    let mut device = twinleaf::Device::open(&proxy, route).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    let meta = device.get_metadata().map_err(|e| {
        eprintln!("Failed to get metadata: {:?}", e);
    })?;

    let output_path = if let Some(path) = matches.opt_str("f") {
        path
    } else {
        "meta.tio".to_string()
    };
    let mut file = File::create(output_path).unwrap();

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

fn log_dump(args: &[String]) -> Result<(), ()> {
    for path in args {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            println!("{:?}", pkt);
        }
    }
    Ok(())
}

fn log_data_dump(args: &[String]) -> Result<(), ()> {
    use twinleaf::data::DeviceDataParser;
    let mut parser = DeviceDataParser::new(args.len() > 1);

    for path in args {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            for sample in parser.process_packet(&pkt) {
                print_sample(&sample, None);
            }
        }
    }
    Ok(())
}

fn log_csv(args: &[String]) -> Result<(), ()> {
    let mut opts = getopts::Options::new();
    opts.optopt(
        "s",
        "",
        "sensor path in the sensor tree (default /)",
        "path",
    );
    opts.optopt("m", "", "metadata file (if separate)", "path");
    opts.optopt("o", "", "output file prefix", "prefix");
    let matches = opts.parse(args).map_err(|e| {
        eprintln!("Invalid invocation: {:?}", e);
    })?;
    if matches.free.len() < 3 {
        eprintln!("Invalid invocation: missing log file");
        return Err(());
    }
    let route = if let Some(path) = matches.opt_str("s") {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };
    let mut parser = if let Some(path) = matches.opt_str("m") {
        let mut parser = DeviceDataParser::new(true);
        let mut meta: &[u8] = &std::fs::read(path).unwrap();
        while meta.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(meta).unwrap();
            meta = &meta[len..];
            for _ in parser.process_packet(&pkt) {}
        }
        parser
    } else {
        DeviceDataParser::new(matches.free.len() > 3)
    };
    let id: u8 = matches.free[1].parse().unwrap();
    let output_path = format!(
        "{}.{}.csv",
        if let Some(path) = matches.opt_str("o") {
            path
        } else {
            matches.free[2].clone()
        },
        id
    );

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path)
        .or(Err(()))?;

    let mut header_written: bool = false;

    for path in &matches.free[2..] {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            if pkt.routing != route {
                continue;
            }
            for sample in parser.process_packet(&pkt) {
                if sample.stream.stream_id != id {
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

fn firmware_upgrade(args: &[String]) -> Result<(), ()> {
    let opts = tio_opts();
    let (matches, root, route) = tio_parseopts(&opts, args);

    if matches.free.len() != 1 {
        panic!("Must specify firmware path only")
    }

    let firmware_data = std::fs::read(matches.free[0].clone()).unwrap();

    println!("Loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Interface::new(&root);
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
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("help".to_string());
    }
    if let Ok(_) = match args[1].as_str() {
        "rpc-list" => list_rpcs(&args[2..]),
        "rpc" => rpc(&args[2..]),
        "rpc-dump" => rpc_dump(&args[2..]),
        "dump" => dump(&args[2..]),
        "log" => log(&args[2..]),
        "log-data" => log_data(&args[2..]),
        "log-metadata" => log_metadata(&args[2..]),
        "log-dump" => log_dump(&args[2..]),
        "log-data-dump" => log_data_dump(&args[2..]),
        "log-csv" => log_csv(&args[1..]),
        "firmware-upgrade" => firmware_upgrade(&args[2..]),
        "data-dump" => data_dump(&args[2..]),
        "all-data-dump" => all_data_dump(&args[2..]),
        "meta-dump" => meta_dump(&args[2..]),
        _ => {
            // TODO: do usage right
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool dump [-r url] [-s sensor]");
            println!(" tio-tool log [-r url] [-s sensor] [-f filename] [-u]");
            println!(" tio-tool log-metadata [-r url] [-s sensor] [-f filename]");
            println!(" tio-tool log-dump filename [filename ...]");
            println!(" tio-tool log-data-dump filename [filename ...]");
            println!(" tio-tool log-csv <stream id> [metadata] <csv>");
            println!(" tio-tool rpc-list [-r url] [-s sensor]");
            println!(" tio-tool rpc [-r url] [-s sensor] [-t type] [-d] <rpc-name> [rpc-arg]");
            println!(" tio-tool rpc-dump [-r url] [-s sensor] <rpc-name>");
            println!(" tio-tool firmware-upgrade [-r url] [-s sensor] <firmware_image.bin>");
            println!(" tio-tool data-dump [-r url] [-s sensor]");
            println!(" tio-tool meta-dump [-r url] [-s sensor]");
            println!(" tio-tool capture <rpc-prefix> <data-type>");
            if args[1] == "help" {
                Ok(())
            } else {
                Err(())
            }
        }
    } {
        ExitCode::SUCCESS
    } else {
        eprintln!("FAILED");
        ExitCode::FAILURE
    }
}

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;

use crate::TioOpts;
use crate::{SplitLevel, SplitPolicy};
use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::DeviceDataParser;
use twinleaf::device::{Device, DeviceTree, RpcClient};
use twinleaf::tio;

fn record_missing_metadata(
    missing_routes: &mut HashSet<DeviceRoute>,
    pkt: &tio::Packet,
    samples_len: usize,
) {
    if samples_len != 0 {
        return;
    }
    if let tio::proto::Payload::StreamData(data) = &pkt.payload {
        if !data.data.is_empty() {
            missing_routes.insert(pkt.routing.clone());
        }
    }
}

fn report_missing_metadata(mut routes: Vec<DeviceRoute>, is_error: bool) {
    if routes.is_empty() {
        return;
    }
    routes.sort();
    let prefix = if is_error { "Error" } else { "Warning" };
    if routes.len() == 1 {
        eprintln!(
            "{}: stream data at route {} could not be parsed because metadata is missing or incompatible.",
            prefix, routes[0]
        );
    } else {
        eprintln!(
            "{}: stream data at these routes could not be parsed because metadata is missing or incompatible:",
            prefix
        );
        for route in routes.iter().take(5) {
            eprintln!("  {}", route);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
    }
    eprintln!("Hint: ensure the log includes metadata or capture it with `tio log metadata`, including it as an argument before the log.");
}

pub fn list_rpcs(tio: &TioOpts) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let rpc_client = RpcClient::open(&proxy, route.clone()).expect("Failed to open RPC client");
    let rpcs = rpc_client.rpc_list(&route).map_err(|e| {
        eprintln!("RPC list failed: {:?}", e);
    })?;

    for (name, _) in rpcs.vec {
        let spec =
            twinleaf::device::util::parse_rpc_spec(*rpcs.map.get(&name).unwrap(), name.to_string());
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
        let spec = twinleaf::device::util::parse_rpc_spec(meta, name.clone());
        spec.type_str()
    } else {
        "".to_string()
    }
}

pub fn rpc(
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

pub fn rpc_dump(tio: &TioOpts, rpc_name: String, is_capture: bool) -> Result<(), ()> {
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

pub fn dump(tio: &TioOpts, data: bool, meta: bool, depth: Option<usize>) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let port_depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);

    let port = proxy
        .new_port(None, route.clone(), port_depth, true, true)
        .map_err(|e| {
            eprintln!("Failed to initialize proxy port: {:?}", e);
        })?;

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            for pkt in port.iter() {
                let abs_pkt = tio::Packet {
                    routing: route.absolute_route(&pkt.routing),
                    ..pkt
                };
                println!("{:?}", abs_pkt);
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            for pkt in port.iter() {
                if let tio::proto::Payload::Metadata(mp) = &pkt.payload {
                    let abs_route = route.absolute_route(&pkt.routing);
                    print_metadata_payload(&abs_route, mp);
                }
            }
        }

        // Sample mode (-d or -d -m): use DeviceTree for parsed samples
        (true, _) => {
            let mut tree = DeviceTree::new(port, route.clone());

            loop {
                match tree.next() {
                    Ok((sample, sample_route)) => {
                        print_sample(&sample, Some(&sample_route), meta, true);
                    }
                    Err(e) => {
                        eprintln!("Device error: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn print_sample(
    sample: &twinleaf::data::Sample,
    route: Option<&DeviceRoute>,
    print_meta: bool,
    print_data: bool,
) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };

    if print_meta {
        if let Some(boundary) = &sample.boundary {
            println!("# {}BOUNDARY {:?}", route_str, boundary.reason);
            if !boundary.is_continuous() {
                println!("# {}DEVICE {:?}", route_str, sample.device);
                println!("# {}STREAM {:?}", route_str, sample.stream);
                for col in &sample.columns {
                    println!("# {}COLUMN {:?}", route_str, col.desc);
                }
            }
            println!("# {}SEGMENT {:?}", route_str, sample.segment);
        }
    }

    if print_data {
        println!("{}{}", route_str, sample);
    }
}

fn print_metadata_payload(route: &DeviceRoute, payload: &tio::proto::MetadataPayload) {
    let route_str = format!("{} ", route);
    match &payload.content {
        tio::proto::meta::MetadataContent::Device(dm) => {
            println!("# {}DEVICE {:?}", route_str, dm);
        }
        tio::proto::meta::MetadataContent::Stream(sm) => {
            println!("# {}STREAM {:?}", route_str, sm);
        }
        tio::proto::meta::MetadataContent::Segment(sm) => {
            println!("# {}SEGMENT {:?}", route_str, sm);
        }
        tio::proto::meta::MetadataContent::Column(cm) => {
            println!("# {}COLUMN {:?}", route_str, cm);
        }
        tio::proto::meta::MetadataContent::Unknown(mtype) => {
            println!("# {}METADATA Unknown({})", route_str, mtype);
        }
    }
}
pub fn log(
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
            .new_port(None, route.clone(), depth, true, true)
            .map_err(|e| {
                eprintln!("Failed to initialize proxy port: {:?}", e);
            })?;

        let mut file = File::create(file).unwrap();
        println!("Logging raw packets...");

        for pkt in port.iter() {
            // Convert relative route to absolute before logging
            let abs_pkt = tio::Packet {
                routing: route.absolute_route(&pkt.routing),
                ..pkt
            };
            let raw = abs_pkt.serialize().unwrap();
            file.write_all(&raw).unwrap();
            if unbuffered {
                file.flush().unwrap();
            }
        }
        return Ok(());
    }

    let mut devs = DeviceTree::open(&proxy, route.clone()).map_err(|e| {
        eprintln!("open failed: {:?}", e);
    })?;

    let mut file = File::create(file).map_err(|e| {
        eprintln!("create failed: {e:?}");
    })?;

    let write_packet = |pkt: tio::Packet, f: &mut File| {
        let _ = f.write_all(&pkt.serialize().unwrap());
    };

    println!("Logging data...");

    loop {
        match devs.drain() {
            Ok(batch) => {
                for (sample, sample_route) in batch {
                    // Write metadata on any boundary (Initial, SessionChanged, SegmentChanged, etc.)
                    if sample.boundary.is_some() {
                        write_packet(
                            sample.device.make_update_with_route(sample_route.clone()),
                            &mut file,
                        );
                        write_packet(
                            sample.stream.make_update_with_route(sample_route.clone()),
                            &mut file,
                        );
                        write_packet(
                            sample.segment.make_update_with_route(sample_route.clone()),
                            &mut file,
                        );
                        for col in &sample.columns {
                            write_packet(
                                col.desc.make_update_with_route(sample_route.clone()),
                                &mut file,
                            );
                        }
                    }

                    // Write data packet once (first sample from this packet)
                    if sample.n == sample.source.first_sample_n {
                        let data_pkt = tio::Packet {
                            payload: tio::proto::Payload::StreamData(sample.source),
                            routing: sample_route,
                            ttl: 0,
                        };
                        write_packet(data_pkt, &mut file);
                    }
                }
            }
            Err(e) => {
                eprintln!("Device error: {e:?}");
                break;
            }
        }

        if unbuffered {
            let _ = file.flush();
        }
    }
    Ok(())
}

pub fn log_metadata(tio: &TioOpts, file: String) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    let mut device = Device::open(&proxy, route.clone()).map_err(|e| {
        eprintln!("Failed to open device: {:?}", e);
    })?;

    let meta = device.get_metadata().map_err(|e| {
        eprintln!("Failed to get metadata: {:?}", e);
    })?;

    let mut file = File::create(file).unwrap();

    file.write_all(
        &meta
            .device
            .make_update_with_route(route.clone())
            .serialize()
            .unwrap(),
    )
    .unwrap();
    for (_id, stream) in meta.streams {
        file.write_all(
            &stream
                .stream
                .make_update_with_route(route.clone())
                .serialize()
                .unwrap(),
        )
        .unwrap();
        file.write_all(
            &stream
                .segment
                .make_update_with_route(route.clone())
                .serialize()
                .unwrap(),
        )
        .unwrap();
        for col in stream.columns {
            file.write_all(
                &col.make_update_with_route(route.clone())
                    .serialize()
                    .unwrap(),
            )
            .unwrap();
        }
    }
    Ok(())
}

pub fn meta_reroute(input: String, route: String, output: Option<String>) -> Result<(), ()> {
    let data = std::fs::read(&input).map_err(|e| {
        eprintln!("Failed to read {}: {}", input, e);
    })?;

    let mut rest: &[u8] = &data;
    let mut routes: HashSet<DeviceRoute> = HashSet::new();
    let mut packet_count = 0usize;

    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::deserialize(rest).map_err(|_| {
            eprintln!("Failed to parse packet in {}", input);
        })?;
        rest = &rest[len..];
        packet_count += 1;

        if !matches!(pkt.payload, tio::proto::Payload::Metadata(_)) {
            eprintln!(
                "Error: {} does not look like a metadata file (found non-metadata packet)",
                input
            );
            return Err(());
        }
        routes.insert(pkt.routing.clone());
    }

    if packet_count == 0 {
        eprintln!("Error: {} contains no packets", input);
        return Err(());
    }

    if routes.len() > 1 {
        let mut routes: Vec<_> = routes.into_iter().collect();
        routes.sort();
        eprintln!("Error: {} contains multiple routes:", input);
        for route in routes.iter().take(5) {
            eprintln!("  {}", route);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        return Err(());
    }

    let new_route = DeviceRoute::from_str(&route).map_err(|_| {
        eprintln!("Invalid route: {}", route);
    })?;
    let output_path = output.unwrap_or_else(|| {
        let base = input.strip_suffix(".tio").unwrap_or(&input);
        format!("{}_rerouted.tio", base)
    });
    if output_path == input {
        eprintln!("Error: output path must be different from input");
        return Err(());
    }

    let mut file = File::create(&output_path).map_err(|e| {
        eprintln!("Failed to create {}: {}", output_path, e);
    })?;

    rest = &data;
    while !rest.is_empty() {
        let (mut pkt, len) = tio::Packet::deserialize(rest).map_err(|_| {
            eprintln!("Failed to parse packet in {}", input);
        })?;
        rest = &rest[len..];
        pkt.routing = new_route.clone();
        let raw = pkt.serialize().map_err(|_| {
            eprintln!("Failed to serialize packet for {}", output_path);
        })?;
        file.write_all(&raw).map_err(|e| {
            eprintln!("Failed to write {}: {}", output_path, e);
        })?;
    }

    Ok(())
}

pub fn log_dump(
    files: Vec<String>,
    data: bool,
    meta: bool,
    sensor: String,
    depth: Option<usize>,
) -> Result<(), ()> {
    if files.is_empty() {
        eprintln!("No input files specified");
        return Err(());
    }

    let target_route = DeviceRoute::from_str(&sensor).unwrap_or_else(|_| DeviceRoute::root());
    let max_depth = depth;

    let route_matches = |route: &DeviceRoute| -> bool {
        match target_route.relative_route(route) {
            Ok(rel) => max_depth.map_or(true, |max| rel.len() <= max),
            Err(_) => false,
        }
    };

    let in_subtree = |route: &DeviceRoute| -> bool { target_route.relative_route(route).is_ok() };

    let mut printed_any = false;
    let mut deeper_routes: HashSet<DeviceRoute> = HashSet::new();

    // Helper to iterate packets from files
    let iter_packets = |files: &[String]| -> Result<Vec<(String, Vec<u8>)>, ()> {
        files
            .iter()
            .map(|path| {
                std::fs::read(path)
                    .map(|data| (path.clone(), data))
                    .map_err(|e| eprintln!("Failed to read {}: {}", path, e))
            })
            .collect()
    };

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = tio::Packet::deserialize(rest).map_err(|_| {
                        eprintln!("Failed to parse packet in {}", path);
                    })?;
                    rest = &rest[len..];

                    if route_matches(&pkt.routing) {
                        println!("{:?}", pkt);
                        printed_any = true;
                    } else if in_subtree(&pkt.routing) {
                        deeper_routes.insert(pkt.routing.clone());
                    }
                }
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            for (_path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(_) => break,
                    };
                    rest = &rest[len..];

                    if let tio::proto::Payload::Metadata(mp) = &pkt.payload {
                        if route_matches(&pkt.routing) {
                            print_metadata_payload(&pkt.routing, mp);
                            printed_any = true;
                        } else if in_subtree(&pkt.routing) {
                            deeper_routes.insert(pkt.routing.clone());
                        }
                    }
                }
            }
        }

        // Sample mode (-d or -d -m): parse and print samples
        (true, _) => {
            let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
            let ignore_session = files.len() > 1;
            let mut missing_metadata_routes: HashSet<DeviceRoute> = HashSet::new();

            for (_path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(_) => break,
                    };
                    rest = &rest[len..];

                    let parser = parsers
                        .entry(pkt.routing.clone())
                        .or_insert_with(|| DeviceDataParser::new(ignore_session));

                    let samples = parser.process_packet(&pkt);
                    record_missing_metadata(&mut missing_metadata_routes, &pkt, samples.len());

                    for sample in samples {
                        if route_matches(&pkt.routing) {
                            print_sample(&sample, Some(&pkt.routing), meta, true);
                            printed_any = true;
                        } else if in_subtree(&pkt.routing) {
                            deeper_routes.insert(pkt.routing.clone());
                        }
                    }
                }
            }

            let missing_routes: Vec<_> = missing_metadata_routes
                .iter()
                .filter(|route| route_matches(route))
                .cloned()
                .collect();
            report_missing_metadata(missing_routes, false);
        }
    }

    // Warn if nothing printed but data exists at deeper routes
    if !printed_any && !deeper_routes.is_empty() {
        let mut routes: Vec<_> = deeper_routes.into_iter().collect();
        routes.sort();
        eprintln!("No data at route {}, but found data at:", sensor);
        for r in routes.iter().take(5) {
            eprintln!("  {}", r);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        eprintln!();
        eprintln!("Use -s to specify a different route, or remove --depth to include all");
    }

    Ok(())
}

pub fn log_csv(
    args: Vec<String>,
    sensor: Option<String>,
    output: Option<String>,
) -> Result<(), ()> {
    if args.is_empty() {
        eprintln!("Invalid invocation: missing stream name and log files");
        eprintln!("Usage: tio log csv <stream> <log.tio>... [-s <route>]");
        return Err(());
    }

    let mut stream_arg: Option<String> = None;
    let mut files: Vec<String> = Vec::new();
    for arg in args {
        if arg.ends_with(".tio") {
            files.push(arg);
        } else if stream_arg.is_none() {
            stream_arg = Some(arg);
        } else {
            eprintln!("Invalid invocation: multiple stream arguments provided");
            eprintln!("Usage: tio log csv <stream> <log.tio>... [-s <route>]");
            eprintln!("Hint: log files should end with .tio");
            return Err(());
        }
    }

    let stream_arg = if let Some(stream) = stream_arg {
        stream
    } else {
        eprintln!("Invalid invocation: missing stream name or id");
        eprintln!("Usage: tio log csv <stream> <log.tio>... [-s <route>]");
        eprintln!("Hint: log files should end with .tio");
        return Err(());
    };

    if files.is_empty() {
        eprintln!("Invalid invocation: missing log file");
        eprintln!("Usage: tio log csv <stream> <log.tio>... [-s <route>]");
        return Err(());
    }

    let target_id = stream_arg.parse::<u8>().ok();

    let target_route = if let Some(path) = sensor {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };

    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;
    let mut missing_metadata_routes: HashSet<DeviceRoute> = HashSet::new();

    let output_path = format!(
        "{}.{}.csv",
        output.unwrap_or_else(|| files.last().cloned().unwrap_or_default()),
        stream_arg
    );

    let mut file: Option<File> = None;
    let mut created_output = false;
    let mut header_written: bool = false;

    for path in &files {
        let file_data = std::fs::read(path).map_err(|e| {
            eprintln!("Failed to read log file {}: {}", path, e);
            eprintln!("Usage: tio log csv <stream> <log.tio>... [-s <route>]");
        })?;
        let mut rest: &[u8] = &file_data;
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).map_err(|_| {
                eprintln!("Failed to parse packet in {}", path);
            })?;
            rest = &rest[len..];

            let parser = parsers
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceDataParser::new(ignore_session));
            let samples = parser.process_packet(&pkt);

            if pkt.routing == target_route {
                record_missing_metadata(&mut missing_metadata_routes, &pkt, samples.len());
            }

            if pkt.routing != target_route {
                continue;
            }

            for sample in samples {
                let is_match = if let Some(id) = target_id {
                    sample.stream.stream_id == id
                } else {
                    sample.stream.name == stream_arg
                };

                if !is_match {
                    continue;
                }

                if !header_written {
                    let mut headers: Vec<String> = vec!["time".to_string()];
                    headers.extend(sample.columns.iter().map(|col| col.desc.name.clone()));

                    if file.is_none() {
                        let existed = std::path::Path::new(&output_path).exists();
                        let opened = OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&output_path)
                            .or(Err(()))?;
                        created_output = !existed;
                        file = Some(opened);
                    }
                    writeln!(file.as_mut().unwrap(), "{}", headers.join(",")).or(Err(()))?;
                    header_written = true;
                }

                let mut values: Vec<String> = Vec::new();
                values.push(format!("{:.6}", sample.timestamp_end()));

                values.extend(sample.columns.iter().map(|col| col.value.to_string()));

                writeln!(file.as_mut().unwrap(), "{}", values.join(",")).or(Err(()))?;
            }
        }
    }

    if !header_written {
        drop(file);
        if created_output {
            std::fs::remove_file(&output_path).ok();
        }
        if missing_metadata_routes.contains(&target_route) {
            report_missing_metadata(vec![target_route.clone()], true);
            return Err(());
        }
        eprintln!(
            "Error: No data found for stream '{}' at route {}",
            stream_arg, target_route
        );
        eprintln!();
        eprintln!("To see available routes and streams, run:");
        eprintln!(
            "  tio log dump -m {}",
            files.first().unwrap_or(&"<file>".to_string())
        );
        return Err(());
    }

    Ok(())
}

#[cfg(feature = "hdf5")]
pub fn log_hdf(
    files: Vec<String>,
    output: Option<String>,
    filter: Option<String>,
    compress: bool,
    debug: bool,
    split_level: SplitLevel,
    split_policy: SplitPolicy,
) -> Result<(), ()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use memmap2::Mmap;
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::Path;
    use twinleaf::data::{export, ColumnFilter, DeviceDataParser};
    use twinleaf::tio;
    use twinleaf::tio::proto::identifiers::StreamKey;

    if files.is_empty() {
        eprintln!("No input files specified");
        return Err(());
    }

    // Determine output filename
    let output = match output {
        Some(o) => o,
        None => {
            let input_path = Path::new(files.last().unwrap_or(&files[0]));
            let stem = input_path.file_stem().unwrap_or_default().to_string_lossy();
            let base = format!("{}.h5", stem);
            if !Path::new(&base).exists() {
                base
            } else {
                (1..=1000)
                    .map(|i| format!("{}_{}.h5", stem, i))
                    .find(|name| !Path::new(name).exists())
                    .ok_or_else(|| eprintln!("Could not find available output filename"))?
            }
        }
    };

    // Parse filter upfront
    let col_filter = if let Some(p) = filter {
        match ColumnFilter::new(&p) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("Filter error: {}", e);
                return Err(());
            }
        }
    } else {
        None
    };

    // Create writer with filter baked in
    let mut writer = export::Hdf5Appender::with_options(
        Path::new(&output),
        compress,
        debug,
        col_filter,
        65_536,
        split_policy.into(),
        split_level.into(),
    )
    .map_err(|e| eprintln!("Failed to create HDF5 file: {:?}", e))?;

    let mut parsers: HashMap<tio::proto::DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;
    let mut missing_metadata_routes: HashSet<tio::proto::DeviceRoute> = HashSet::new();

    println!("Processing {} files...", files.len());

    for path in &files {
        let file = File::open(&path).map_err(|e| eprintln!("Open failed: {:?}", e))?;
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| eprintln!("Mmap failed: {:?}", e))?;

        let total_bytes = mmap.len() as u64;
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message(path.clone());

        let mut rest: &[u8] = &mmap[..];

        while !rest.is_empty() {
            let (pkt, len) = match tio::Packet::deserialize(rest) {
                Ok(res) => res,
                Err(_) => break,
            };
            rest = &rest[len..];
            pb.set_position(total_bytes - rest.len() as u64);

            let parser = parsers
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceDataParser::new(ignore_session));

            let samples = parser.process_packet(&pkt);
            record_missing_metadata(&mut missing_metadata_routes, &pkt, samples.len());

            for sample in samples {
                let key = StreamKey::new(pkt.routing.clone(), sample.stream.stream_id);

                if debug {
                    if let Some(ref boundary) = sample.boundary {
                        eprintln!(
                            "[{}] sample_n={} boundary={:?}",
                            sample.stream.name, sample.n, boundary.reason
                        );
                    }
                }

                if let Err(e) = writer.write_sample(sample, key) {
                    eprintln!("HDF5 Write Error: {:?}", e);
                    return Err(());
                }
            }
        }

        pb.finish_with_message("Completed");
    }

    // Finish flushes all pending data and returns stats
    let stats = writer
        .finish()
        .map_err(|e| eprintln!("Failed to finalize HDF5: {:?}", e))?;

    report_missing_metadata(missing_metadata_routes.into_iter().collect(), false);

    let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    let duration = stats.end_time.unwrap_or(0.0) - stats.start_time.unwrap_or(0.0);
    let size_mb = file_size as f64 / 1_048_576.0;

    println!("\n--------------------------------------------------");
    println!(" Export Summary");
    println!("--------------------------------------------------");
    println!(" Output File:     {}", output);
    println!(" File Size:       {:.2} MB", size_mb);
    println!(" Duration:        {:.3} s", duration);
    println!(" Total Samples:   {}", stats.total_samples);
    println!(" Streams Written: {}", stats.streams_written.len());

    if !stats.streams_written.is_empty() {
        println!("\n Active Streams:");
        let mut streams: Vec<_> = stats.streams_written.into_iter().collect();
        streams.sort();
        for stream in streams {
            println!("  • {}", stream);
        }
    }
    println!("--------------------------------------------------");

    Ok(())
}

#[cfg(not(feature = "hdf5"))]
pub fn log_hdf(
    _files: Vec<String>,
    _output: Option<String>,
    _filter: Option<String>,
    _compress: bool,
    _debug: bool,
    _split_level: SplitLevel,
    _split_policy: SplitPolicy,
) -> Result<(), ()> {
    eprintln!("Error: This version of twinleaf-tools was compiled without HDF5 support.");
    eprintln!("To enable it, reinstall with:");
    eprintln!("  cargo install twinleaf-tools --features hdf5");
    Err(())
}

pub fn firmware_upgrade(
    tio: &TioOpts,
    firmware_path: String,
    skip_confirm: bool,
) -> Result<(), ()> {
    use indicatif::{ProgressBar, ProgressStyle};

    let firmware_data = std::fs::read(firmware_path).unwrap();

    println!("Loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();
    let device = proxy.device_rpc(route).unwrap();

    let dev_name: String = match device.rpc("dev.name", ()) {
        Ok(name) => name,
        Err(e) => {
            eprintln!("Failed to query device name: {:?}", e);
            return Err(());
        }
    };

    if !skip_confirm {
        print!("Upgrade firmware on '{}'? [y/N] ", dev_name);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    match device.action("dev.stop") {
        Ok(()) => {}
        Err(proxy::RpcError::ExecError(ref e))
            if matches!(
                e.error,
                tio::proto::RpcErrorCode::NotFound | tio::proto::RpcErrorCode::WrongDeviceState
            ) => {}
        Err(e) => {
            eprintln!("Failed to stop device: {:?}", e);
            return Err(());
        }
    }

    let total_chunks = firmware_data.len().div_ceil(288);
    let pb = ProgressBar::new(total_chunks as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent}%")
            .unwrap()
            .progress_chars("#>-"),
    );

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
                next_ack_chunk += 1;
                pb.set_position(next_ack_chunk as u64);
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

    pb.finish_and_clear();

    if let Err(_) = device.action("dev.firmware.upgrade") {
        panic!("upgrade failed");
    }

    // Wait 5 seconds before returning to ensure the device is not
    // power-cycled while the firmware upgrade is being committed to flash.
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message("Finalizing upgrade...");
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        spinner.tick();
    }
    spinner.finish_with_message("Firmware upgrade complete.");

    Ok(())
}

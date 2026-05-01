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
use twinleaf::device::util::{rpc_decode_reply, rpc_encode_arg};
use twinleaf::device::{Device, DeviceTree, RpcClient, RpcValue, RpcValueType};
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

pub fn list_rpcs(tio: &TioOpts) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let rpc_client = RpcClient::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open RPC client for {}", tio.root))?;
    let rpcs = rpc_client
        .rpc_list(&route)
        .wrap_err("failed to query RPC list")?;

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

fn infer_rpc_type(name: &str, device: &proxy::Port, kind: &str) -> RpcValueType {
    let meta: Option<u16> = device.rpc("rpc.info", &name.to_string()).ok();
    if meta.is_none() {
        println!("Unknown RPC {kind} type, assuming 'string'. Use -t/-T to override.");
    }
    twinleaf::device::util::resolve_arg_type(meta, name)
}

pub fn rpc(
    tio: &TioOpts,
    rpc_name: String,
    rpc_arg: Option<String>,
    req_type: Option<RpcValueType>,
    rep_type: Option<RpcValueType>,
    debug: bool,
) -> eyre::Result<()> {
    use eyre::WrapErr;

    let (status_send, proxy_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let proxy = proxy::Interface::new_proxy(&tio.root, None, Some(status_send));
    let route = tio.route.clone();
    let device = proxy
        .device_rpc(route)
        .wrap_err_with(|| format!("could not open device at {}", tio.root))?;

    let req_type = req_type.or_else(|| {
        rpc_arg
            .is_some()
            .then(|| infer_rpc_type(&rpc_name, &device, "arg"))
    });

    let arg_bytes = match (rpc_arg.as_deref(), req_type.as_ref()) {
        (None, _) => Vec::new(),
        (Some(s), Some(t)) => rpc_encode_arg(s, t)
            .wrap_err_with(|| format!("could not encode argument for RPC {}", rpc_name))?,
        (Some(_), None) => unreachable!("req_type is set whenever rpc_arg is present"),
    };

    let reply = match device.raw_rpc(&rpc_name, &arg_bytes) {
        Ok(rep) => rep,
        Err(err) => {
            drop(proxy);
            if debug {
                for s in proxy_status.try_iter() {
                    println!("{:?}", s);
                }
            }
            return Err(eyre::Report::new(err).wrap_err(format!("RPC {} failed", rpc_name)));
        }
    };

    if !reply.is_empty() {
        let rep_type = rep_type
            .or(req_type)
            .unwrap_or_else(|| infer_rpc_type(&rpc_name, &device, "ret"));
        let value = rpc_decode_reply(&reply, &rep_type)
            .wrap_err_with(|| format!("could not decode reply from RPC {}", rpc_name))?;
        let formatted = match &value {
            RpcValue::Str(s) => format!("\"{}\" {:?}", s, s.as_bytes()),
            RpcValue::Bytes(b) => format!("{:?}", b),
            other => format!("{}", other),
        };
        println!("Reply: {}", formatted);
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

pub fn rpc_dump(tio: &TioOpts, rpc_name: String, is_capture: bool) -> eyre::Result<()> {
    use eyre::WrapErr;

    let rpc_name = if is_capture {
        rpc_name.clone() + ".block"
    } else {
        rpc_name.clone()
    };

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let device = proxy
        .device_rpc(route)
        .wrap_err_with(|| format!("could not open device at {}", tio.root))?;

    if is_capture {
        let trigger_rpc_name = rpc_name[..rpc_name.len() - 6].to_string() + ".trigger";
        device
            .action(&trigger_rpc_name)
            .wrap_err_with(|| format!("failed to trigger {}", trigger_rpc_name))?;
    }

    let mut full_reply = vec![];

    for i in 0u16..=65535u16 {
        match device.raw_rpc(&rpc_name, &i.to_le_bytes().to_vec()) {
            Ok(mut rep) => full_reply.append(&mut rep),
            Err(proxy::RpcError::ExecError(err)) => {
                if let tio::proto::RpcErrorCode::InvalidArgs = err.error {
                    break;
                } else {
                    return Err(eyre::Report::new(proxy::RpcError::ExecError(err))
                        .wrap_err(format!("RPC {} failed at chunk {}", rpc_name, i)));
                }
            }
            Err(e) => {
                return Err(eyre::Report::new(e)
                    .wrap_err(format!("RPC {} failed at chunk {}", rpc_name, i)));
            }
        }
    }

    if let Ok(s) = std::str::from_utf8(&full_reply) {
        println!("{}", s);
    } else {
        std::io::stdout()
            .write(&full_reply)
            .wrap_err("failed to write dump to stdout")?;
    }
    Ok(())
}

pub fn dump(tio: &TioOpts, data: bool, meta: bool, depth: Option<usize>) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let port_depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);

    let port = proxy
        .new_port(None, route.clone(), port_depth, true, true)
        .wrap_err_with(|| format!("could not open port on {}", tio.root))?;

    eprintln!("Dumping from {} (route {})...", tio.root, route);

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
                        return Err(eyre::Report::new(e).wrap_err("device stream ended"));
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
    duration: Option<std::time::Duration>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};
    use std::path::Path;
    use std::time::{Duration, Instant};
    use twinleaf::data::BoundaryReason;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();

    let file_name = Path::new(&file)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();

    let template = match duration {
        Some(d) => {
            let s = d.as_secs();
            format!(
                "{{spinner}} [{{elapsed_precise}}/{:02}:{:02}:{:02}] {{decimal_bytes}} → {{msg}}",
                s / 3600,
                (s / 60) % 60,
                s % 60,
            )
        }
        None => "{spinner} [{elapsed_precise}] {decimal_bytes} → {msg}".to_string(),
    };
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(&template)
            .unwrap()
            .tick_strings(&["←", "↖", "↑", "↗", "→", "↘", "↓", "↙", " "]),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let started = Instant::now();
    let mut bytes_written: u64 = 0;
    let mut samples_dropped: u64 = 0;

    let static_msg = {
        let mut parts: Vec<String> = vec![file_name.clone()];
        if raw {
            parts.push("raw".into());
        }
        if unbuffered {
            parts.push("unbuf".into());
        }
        parts.join(" · ")
    };
    let render_msg = |dropped: u64| -> String {
        if dropped > 0 {
            format!("{} · ({} dropped)", static_msg, dropped)
        } else {
            static_msg.clone()
        }
    };
    pb.set_message(render_msg(0));

    let duration_elapsed = || duration.is_some_and(|d| started.elapsed() >= d);

    if raw {
        let port_depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);
        let port = proxy
            .new_port(None, route.clone(), port_depth, true, true)
            .wrap_err_with(|| format!("could not open port on {}", tio.root))?;

        let mut file_out =
            File::create(&file).wrap_err_with(|| format!("could not create log file {}", file))?;

        for pkt in port.iter() {
            if duration_elapsed() {
                break;
            }
            let abs_pkt = tio::Packet {
                routing: route.absolute_route(&pkt.routing),
                ..pkt
            };
            let serialized = abs_pkt
                .serialize()
                .map_err(|_| eyre::eyre!("failed to serialize packet for log"))?;
            file_out
                .write_all(&serialized)
                .wrap_err_with(|| format!("failed to write {}", file))?;
            bytes_written += serialized.len() as u64;
            pb.set_position(bytes_written);
            pb.set_message(render_msg(samples_dropped));
            if unbuffered {
                file_out
                    .flush()
                    .wrap_err_with(|| format!("failed to flush {}", file))?;
            }
        }
        pb.finish_and_clear();
        return Ok(());
    }

    let mut devs = DeviceTree::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open device tree on {}", tio.root))?;

    let mut file_out =
        File::create(&file).wrap_err_with(|| format!("could not create log file {}", file))?;

    let write_packet = |pkt: tio::Packet, f: &mut File, b: &mut u64| {
        let serialized = pkt.serialize().unwrap();
        let _ = f.write_all(&serialized);
        *b += serialized.len() as u64;
    };

    loop {
        if duration_elapsed() {
            break;
        }
        match devs.drain() {
            Ok(batch) => {
                for (sample, sample_route) in batch {
                    if let Some(b) = &sample.boundary {
                        if let BoundaryReason::SamplesLost { expected, received } = b.reason {
                            samples_dropped += received.wrapping_sub(expected) as u64;
                        }
                        write_packet(
                            sample.device.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        );
                        write_packet(
                            sample.stream.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        );
                        write_packet(
                            sample.segment.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        );
                        for col in &sample.columns {
                            write_packet(
                                col.desc.make_update_with_route(sample_route.clone()),
                                &mut file_out,
                                &mut bytes_written,
                            );
                        }
                    }

                    if sample.n == sample.source.first_sample_n {
                        let data_pkt = tio::Packet {
                            payload: tio::proto::Payload::StreamData(sample.source),
                            routing: sample_route,
                            ttl: 0,
                        };
                        write_packet(data_pkt, &mut file_out, &mut bytes_written);
                    }
                }
                pb.set_position(bytes_written);
                pb.set_message(render_msg(samples_dropped));
            }
            Err(e) => {
                pb.finish_and_clear();
                return Err(eyre::Report::new(e).wrap_err("device stream ended"));
            }
        }

        if unbuffered {
            let _ = file_out.flush();
        }
    }
    pb.finish_and_clear();
    Ok(())
}

pub fn log_metadata(tio: &TioOpts, file: String) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();

    let mut device = Device::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open device at {}", tio.root))?;

    let meta = device
        .get_metadata()
        .wrap_err("failed to fetch device metadata")?;

    let mut file_out =
        File::create(&file).wrap_err_with(|| format!("could not create {}", file))?;

    let write_packet = |f: &mut File, pkt: tio::Packet| -> eyre::Result<()> {
        let raw = pkt
            .serialize()
            .map_err(|_| eyre::eyre!("failed to serialize metadata packet"))?;
        f.write_all(&raw)
            .wrap_err_with(|| format!("failed to write {}", file))
    };

    write_packet(
        &mut file_out,
        meta.device.make_update_with_route(route.clone()),
    )?;
    for (_id, stream) in meta.streams {
        write_packet(
            &mut file_out,
            stream.stream.make_update_with_route(route.clone()),
        )?;
        write_packet(
            &mut file_out,
            stream.segment.make_update_with_route(route.clone()),
        )?;
        for col in stream.columns {
            write_packet(&mut file_out, col.make_update_with_route(route.clone()))?;
        }
    }
    Ok(())
}

pub fn meta_reroute(input: String, route: String, output: Option<String>) -> eyre::Result<()> {
    use eyre::{bail, WrapErr};

    let data = std::fs::read(&input).wrap_err_with(|| format!("could not read {}", input))?;

    let mut rest: &[u8] = &data;
    let mut routes: HashSet<DeviceRoute> = HashSet::new();
    let mut packet_count = 0usize;

    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::deserialize(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        packet_count += 1;

        if !matches!(pkt.payload, tio::proto::Payload::Metadata(_)) {
            bail!(
                "{} does not look like a metadata file (found non-metadata packet)",
                input
            );
        }
        routes.insert(pkt.routing.clone());
    }

    if packet_count == 0 {
        bail!("{} contains no packets", input);
    }

    if routes.len() > 1 {
        let mut routes: Vec<_> = routes.into_iter().collect();
        routes.sort();
        eprintln!("{} contains multiple routes:", input);
        for route in routes.iter().take(5) {
            eprintln!("  {}", route);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        bail!("cannot reroute a file with multiple routes");
    }

    let new_route =
        DeviceRoute::from_str(&route).map_err(|_| eyre::eyre!("invalid route: {}", route))?;
    let output_path = output.unwrap_or_else(|| {
        let base = input.strip_suffix(".tio").unwrap_or(&input);
        format!("{}_rerouted.tio", base)
    });
    if output_path == input {
        bail!("output path must be different from input");
    }

    let mut file =
        File::create(&output_path).wrap_err_with(|| format!("could not create {}", output_path))?;

    rest = &data;
    while !rest.is_empty() {
        let (mut pkt, len) = tio::Packet::deserialize(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        pkt.routing = new_route.clone();
        let raw = pkt
            .serialize()
            .map_err(|_| eyre::eyre!("failed to serialize packet for {}", output_path))?;
        file.write_all(&raw)
            .wrap_err_with(|| format!("failed to write {}", output_path))?;
    }

    Ok(())
}

pub fn log_dump(
    files: Vec<String>,
    data: bool,
    meta: bool,
    sensor: String,
    depth: Option<usize>,
) -> eyre::Result<()> {
    use eyre::WrapErr;

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
    let iter_packets = |files: &[String]| -> eyre::Result<Vec<(String, Vec<u8>)>> {
        files
            .iter()
            .map(|path| {
                std::fs::read(path)
                    .map(|data| (path.clone(), data))
                    .wrap_err_with(|| format!("could not read {}", path))
            })
            .collect()
    };

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = tio::Packet::deserialize(rest)
                        .wrap_err_with(|| format!("could not parse packet in {}", path))?;
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

fn data_type_label(dt: &tio::proto::DataType) -> String {
    use tio::proto::DataType;
    match dt {
        DataType::UInt8 => "u8".to_string(),
        DataType::Int8 => "i8".to_string(),
        DataType::UInt16 => "u16".to_string(),
        DataType::Int16 => "i16".to_string(),
        DataType::UInt24 => "u24".to_string(),
        DataType::Int24 => "i24".to_string(),
        DataType::UInt32 => "u32".to_string(),
        DataType::Int32 => "i32".to_string(),
        DataType::UInt64 => "u64".to_string(),
        DataType::Int64 => "i64".to_string(),
        DataType::Float32 => "f32".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Unknown(n) => format!("raw{}", n),
    }
}

fn fmt_hms(secs: f64) -> String {
    if !secs.is_finite() || secs <= 0.0 {
        return "0s".to_string();
    }
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = secs - (h * 3600 + m * 60) as f64;
    if h > 0 {
        format!("{}h{:02}m{:04.1}s", h, m, s)
    } else if m > 0 {
        format!("{}m{:04.1}s", m, s)
    } else {
        format!("{:.1}s", s)
    }
}

pub fn log_inspect(files: Vec<String>) -> eyre::Result<()> {
    for (i, path) in files.iter().enumerate() {
        if i > 0 {
            println!();
        }
        inspect_one_log(path)?;
    }
    Ok(())
}

fn inspect_one_log(path: &str) -> eyre::Result<()> {
    use console::style;
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};
    use memmap2::Mmap;
    use std::collections::BTreeMap;
    use twinleaf::data::{BoundaryReason, DeviceDataParser};
    use twinleaf::tio::proto::identifiers::StreamId;

    let file = File::open(path).wrap_err_with(|| format!("could not open {}", path))?;
    let mmap = unsafe { Mmap::map(&file) }.wrap_err_with(|| format!("could not mmap {}", path))?;
    let total_bytes = mmap.len() as u64;

    let pb = if total_bytes > 10 * 1024 * 1024 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    struct StreamAgg {
        name: String,
        rate_hz: f64,
        sample_count: u64,
        first_t: Option<f64>,
        last_t: Option<f64>,
        columns: Vec<(String, String, String)>,
    }

    struct DeviceAgg {
        name: String,
        firmware: String,
        serial: String,
    }

    let mut parsers: HashMap<tio::proto::DeviceRoute, DeviceDataParser> = HashMap::new();
    let mut streams: BTreeMap<(tio::proto::DeviceRoute, StreamId), StreamAgg> = BTreeMap::new();
    let mut devices: BTreeMap<tio::proto::DeviceRoute, DeviceAgg> = BTreeMap::new();
    let mut packet_count: u64 = 0;
    let mut session_changes: u64 = 0;
    let mut segment_changes: u64 = 0;

    let mut rest: &[u8] = &mmap[..];
    while !rest.is_empty() {
        let (pkt, len) = match tio::Packet::deserialize(rest) {
            Ok(r) => r,
            Err(_) => break,
        };
        rest = &rest[len..];
        packet_count += 1;
        if let Some(pb) = &pb {
            pb.set_position(total_bytes - rest.len() as u64);
        }

        let parser = parsers
            .entry(pkt.routing.clone())
            .or_insert_with(|| DeviceDataParser::new(false));
        let samples = parser.process_packet(&pkt);

        for sample in samples {
            devices
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceAgg {
                    name: sample.device.name.clone(),
                    firmware: sample.device.firmware_hash.clone(),
                    serial: sample.device.serial_number.clone(),
                });

            let key = (pkt.routing.clone(), sample.stream.stream_id);
            let entry = streams.entry(key).or_insert_with(|| {
                let decim = sample.segment.decimation.max(1);
                let rate = f64::from(sample.segment.sampling_rate) / f64::from(decim);
                let cols = sample
                    .columns
                    .iter()
                    .map(|c| {
                        (
                            c.desc.name.clone(),
                            data_type_label(&c.desc.data_type),
                            c.desc.units.clone(),
                        )
                    })
                    .collect();
                StreamAgg {
                    name: sample.stream.name.clone(),
                    rate_hz: rate,
                    sample_count: 0,
                    first_t: None,
                    last_t: None,
                    columns: cols,
                }
            });
            entry.sample_count += 1;
            let t = sample.timestamp_end();
            entry.first_t = Some(entry.first_t.map_or(t, |p| p.min(t)));
            entry.last_t = Some(entry.last_t.map_or(t, |p| p.max(t)));

            if let Some(boundary) = &sample.boundary {
                match boundary.reason {
                    BoundaryReason::SessionChanged { .. } => session_changes += 1,
                    BoundaryReason::SegmentChanged { .. } => segment_changes += 1,
                    _ => {}
                }
            }
        }
    }

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    let size_mib = total_bytes as f64 / 1_048_576.0;
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:12}", s)).bold().cyan();
    let unit = |s: &str| style(s.to_string()).dim();

    println!();
    println!("{rule}");
    println!(" {}", style("Log Inspection").bold());
    println!("{rule}");
    println!(" {} {}", label("File:"), path);
    println!(" {} {:.2} {}", label("Size:"), size_mib, unit("MiB"));
    println!(" {} {}", label("Packets:"), packet_count);

    println!();
    println!(" {}", style("Devices:").bold().cyan());
    if devices.is_empty() {
        println!("   (no device metadata seen)");
    } else {
        for (route, d) in &devices {
            println!(
                "   • {}  {}  {}  {}",
                route,
                d.name,
                style(format!("fw {}", d.firmware)).dim(),
                style(format!("serial {}", d.serial)).dim(),
            );
        }
    }

    println!();
    println!(" {}", style("Streams:").bold().cyan());
    if streams.is_empty() {
        println!("   (no sample data seen)");
    } else {
        for ((route, sid), s) in &streams {
            let declared = if s.rate_hz > 0.0 {
                s.sample_count as f64 / s.rate_hz
            } else {
                0.0
            };
            let observed = match (s.first_t, s.last_t) {
                (Some(a), Some(b)) => b - a,
                _ => 0.0,
            };
            let skew = if declared > 0.0 {
                (declared - observed).abs() / declared
            } else {
                0.0
            };
            let trailing = if skew > 0.05 && declared > 0.0 {
                format!(
                    "  {}",
                    style(format!(
                        "⚠ declared {} vs observed {}",
                        fmt_hms(declared),
                        fmt_hms(observed)
                    ))
                    .yellow()
                )
            } else {
                String::new()
            };
            println!(
                "   • {} {} {:<12} {:>6.0} {}  {:>4} {}  {:>10} {}{}",
                route,
                sid,
                s.name,
                s.rate_hz,
                unit("Hz"),
                s.columns.len(),
                unit("cols"),
                s.sample_count,
                unit("samples"),
                trailing,
            );
            if !s.columns.is_empty() {
                let cols: Vec<String> = s
                    .columns
                    .iter()
                    .map(|(n, t, u)| {
                        if u.is_empty() {
                            format!("{} {}", n, style(t).dim())
                        } else {
                            format!("{} {} {}", n, style(t).dim(), style(u).dim())
                        }
                    })
                    .collect();
                println!("        {}", cols.join(", "));
            }
        }
    }

    println!();
    let boundary_text = format!(
        "{} session changes, {} segment changes",
        session_changes, segment_changes
    );
    let styled_boundaries = if session_changes > 0 || segment_changes > 0 {
        style(boundary_text).yellow().to_string()
    } else {
        boundary_text
    };
    println!(" {} {}", label("Boundaries:"), styled_boundaries);
    println!("{rule}");

    Ok(())
}

pub fn log_csv(
    args: Vec<String>,
    sensor: Option<String>,
    output: Option<String>,
) -> eyre::Result<()> {
    use color_eyre::Help;
    use eyre::{bail, WrapErr};

    let usage_hint = "tio log csv <stream> <log.tio>... [-s <route>]";

    if args.is_empty() {
        return Err(eyre::eyre!("missing stream name and log files").suggestion(usage_hint));
    }

    let mut stream_arg: Option<String> = None;
    let mut files: Vec<String> = Vec::new();
    for arg in args {
        if arg.ends_with(".tio") {
            files.push(arg);
        } else if stream_arg.is_none() {
            stream_arg = Some(arg);
        } else {
            return Err(eyre::eyre!("multiple stream arguments provided")
                .suggestion(usage_hint)
                .suggestion("log files should end with .tio"));
        }
    }

    let stream_arg = stream_arg.ok_or_else(|| {
        eyre::eyre!("missing stream name or id")
            .suggestion(usage_hint)
            .suggestion("log files should end with .tio")
    })?;

    if files.is_empty() {
        return Err(eyre::eyre!("missing log file").suggestion(usage_hint));
    }

    let target_id = stream_arg.parse::<u8>().ok();

    let target_route = if let Some(path) = sensor {
        DeviceRoute::from_str(&path).map_err(|_| eyre::eyre!("invalid route: {}", path))?
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
        let file_data = std::fs::read(path)
            .wrap_err_with(|| format!("could not read log file {}", path))
            .suggestion(usage_hint)?;
        let mut rest: &[u8] = &file_data;
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest)
                .wrap_err_with(|| format!("could not parse packet in {}", path))?;
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
                            .wrap_err_with(|| format!("could not open {}", output_path))?;
                        created_output = !existed;
                        file = Some(opened);
                    }
                    writeln!(file.as_mut().unwrap(), "{}", headers.join(","))
                        .wrap_err_with(|| format!("failed to write {}", output_path))?;
                    header_written = true;
                }

                let mut values: Vec<String> = Vec::new();
                values.push(format!("{:.6}", sample.timestamp_end()));

                values.extend(sample.columns.iter().map(|col| col.value.to_string()));

                writeln!(file.as_mut().unwrap(), "{}", values.join(","))
                    .wrap_err_with(|| format!("failed to write {}", output_path))?;
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
            bail!("no metadata found for target route");
        }
        return Err(eyre::eyre!(
            "no data found for stream '{}' at route {}",
            stream_arg,
            target_route
        )
        .suggestion(format!(
            "see available routes and streams with: tio log dump -m {}",
            files.first().unwrap_or(&"<file>".to_string())
        )));
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
) -> eyre::Result<()> {
    use eyre::{bail, WrapErr};
    use indicatif::{ProgressBar, ProgressStyle};
    use memmap2::Mmap;
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::Path;
    use twinleaf::data::{export, ColumnFilter, DeviceDataParser};
    use twinleaf::tio;
    use twinleaf::tio::proto::identifiers::StreamKey;

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
                    .ok_or_else(|| eyre::eyre!("could not find available output filename"))?
            }
        }
    };

    let filter_pattern = filter.clone();
    let col_filter = if let Some(p) = filter {
        Some(ColumnFilter::new(&p).map_err(|e| eyre::eyre!("invalid column filter: {}", e))?)
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
        split_policy.clone().into(),
        split_level.clone().into(),
    )
    .wrap_err_with(|| format!("could not create HDF5 file {}", output))?;

    let mut parsers: HashMap<tio::proto::DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;
    let mut missing_metadata_routes: HashSet<tio::proto::DeviceRoute> = HashSet::new();
    let mut total_input_bytes: u64 = 0;

    println!("Processing {} files...", files.len());

    for path in &files {
        let file = File::open(&path).wrap_err_with(|| format!("could not open {}", path))?;
        let mmap =
            unsafe { Mmap::map(&file) }.wrap_err_with(|| format!("could not mmap {}", path))?;

        let total_bytes = mmap.len() as u64;
        total_input_bytes += total_bytes;
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

                writer
                    .write_sample(sample, key)
                    .wrap_err("failed to write sample to HDF5")?;
            }
        }

        pb.finish_with_message("Completed");
    }

    // Finish flushes all pending data and returns stats
    let stats = writer.finish().wrap_err("failed to finalize HDF5")?;

    report_missing_metadata(missing_metadata_routes.into_iter().collect(), false);

    use console::style;

    let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    let size_mib = file_size as f64 / 1_048_576.0;
    let input_mib = total_input_bytes as f64 / 1_048_576.0;
    let written = stats.streams_written.len();
    let seen = stats.streams_seen.len();
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:10}", s)).bold().cyan();
    let unit = |s: &str| style(s.to_string()).dim();

    println!();
    println!("{rule}");
    println!(" {}", style("HDF5 Output Summary").bold());
    println!("{rule}");
    println!(" {} {}", label("Output:"), output);

    if compress && total_input_bytes > 0 {
        let ratio = total_input_bytes as f64 / file_size.max(1) as f64;
        println!(
            " {} {:.2} {}  ({:.1}× smaller, from {:.2} {})",
            label("Size:"),
            size_mib,
            unit("MiB"),
            ratio,
            input_mib,
            unit("MiB"),
        );
    } else {
        println!(" {} {:.2} {}", label("Size:"), size_mib, unit("MiB"));
    }
    println!(" {} {}", label("Samples:"), stats.total_samples);

    match filter_pattern.as_deref() {
        Some(pat) if written == 0 && seen > 0 => {
            println!(
                " {} {}",
                label("Streams:"),
                style(format!(
                    "⚠ 0 of {} matching \"{}\" — none matched; check pattern",
                    seen, pat
                ))
                .yellow(),
            );
        }
        Some(pat) => {
            println!(
                " {} {} of {} matching \"{}\"",
                label("Streams:"),
                written,
                seen,
                pat
            );
        }
        None => {
            println!(" {} {}", label("Streams:"), written);
        }
    }

    if !matches!(split_level, SplitLevel::None) {
        let mode = match split_level {
            SplitLevel::Stream => "per-stream",
            SplitLevel::Device => "per-device",
            SplitLevel::Global => "global",
            SplitLevel::None => unreachable!(),
        };
        println!(
            " {} {}  ({} discontinuities detected)",
            label("Split:"),
            mode,
            stats.discontinuities_detected
        );
    }

    if matches!(split_policy, SplitPolicy::Monotonic) {
        println!(" {} monotonic", label("Policy:"));
    }

    println!("{rule}");

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
) -> eyre::Result<()> {
    use color_eyre::Help;
    Err(
        eyre::eyre!("this version of twinleaf-tools was compiled without HDF5 support")
            .suggestion("reinstall with: cargo install twinleaf-tools --features hdf5"),
    )
}

pub fn firmware_upgrade(
    tio: &TioOpts,
    firmware_path: std::path::PathBuf,
    skip_confirm: bool,
) -> eyre::Result<()> {
    use color_eyre::Help;
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};

    let firmware_data = std::fs::read(&firmware_path)
        .wrap_err_with(|| format!("could not read firmware file {:?}", firmware_path))?;

    println!("Loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let device = proxy
        .device_rpc(route)
        .wrap_err_with(|| format!("could not open device at {}", tio.root))?;

    let dev_name: String = device
        .rpc("dev.name", ())
        .wrap_err("failed to query device name")?;

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
            return Err(
                eyre::Report::new(e).wrap_err("failed to stop device before firmware upgrade")
            );
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

    let power_cycle_hint = "power cycle the device before retrying";

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

            device
                .send(util::PacketBuilder::make_rpc_request(
                    "dev.firmware.upload",
                    &firmware_data[offset..chunk_end],
                    next_send_chunk,
                    DeviceRoute::root(),
                ))
                .wrap_err_with(|| {
                    format!(
                        "failed to send firmware chunk {}/{}",
                        next_send_chunk + 1,
                        total_chunks
                    )
                })
                .suggestion(power_cycle_hint)?;
            next_send_chunk += 1;
            more_to_send = chunk_end < firmware_data.len();
        }

        let pkt = if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            match device.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => continue,
                Err(e) => {
                    return Err(eyre::Report::new(e)
                        .wrap_err("failed to receive firmware upload ack")
                        .suggestion(power_cycle_hint));
                }
            }
        } else {
            device
                .recv()
                .wrap_err("failed to receive firmware upload ack")
                .suggestion(power_cycle_hint)?
        };

        match pkt.payload {
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id != next_ack_chunk {
                    return Err(eyre::eyre!(
                        "firmware chunk ack out of order (expected {}, got {})",
                        next_ack_chunk,
                        rep.id
                    )
                    .suggestion(power_cycle_hint));
                }
                next_ack_chunk += 1;
                pb.set_position(next_ack_chunk as u64);
            }
            tio::proto::Payload::RpcError(err) => {
                return Err(eyre::eyre!(
                    "device rejected firmware chunk {}/{}: {}",
                    next_ack_chunk + 1,
                    total_chunks,
                    err.error
                )
                .suggestion(power_cycle_hint));
            }
            _ => continue,
        }
    }

    pb.finish_and_clear();

    device
        .action("dev.firmware.upgrade")
        .wrap_err("device rejected firmware commit")
        .suggestion(power_cycle_hint)?;

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

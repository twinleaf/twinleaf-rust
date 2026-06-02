use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;

use crate::tools::dump::{print_metadata_payload, print_sample};
use crate::{
    parse_csv_target, LogCli, LogSubcommands, MetaSubcommands, ProxyHelp, SplitLevel, SplitPolicy,
    StreamSel, TioOpts,
};
use twinleaf::data::DeviceDataParser;
use twinleaf::device::{Device, DeviceRoute, DeviceTree};
use twinleaf::tio::{self, proxy};

pub fn run_log(log_cli: LogCli) -> eyre::Result<()> {
    match log_cli.subcommands {
        Some(LogSubcommands::Meta {
            tio,
            subcommands,
            file,
        }) => match subcommands {
            Some(MetaSubcommands::Reroute {
                input,
                route,
                output,
            }) => meta_reroute(input, route, output),
            None => log_metadata(&tio, file),
        },
        Some(LogSubcommands::Dump {
            files,
            data,
            meta,
            sensor,
            depth,
        }) => log_dump(files, data, meta, sensor, depth),
        Some(LogSubcommands::Inspect { files }) => log_inspect(files),
        Some(LogSubcommands::Csv {
            args,
            sensor,
            output,
            force,
        }) => log_csv(args, sensor, output, force),
        Some(LogSubcommands::Hdf {
            files,
            output,
            filter,
            compress,
            debug,
            split_level,
            split_policy,
        }) => log_hdf(
            files,
            output,
            filter,
            compress,
            debug,
            split_level,
            split_policy,
        ),
        None => log(
            &log_cli.tio,
            log_cli.file,
            log_cli.unbuffered,
            log_cli.raw,
            log_cli.depth,
            log_cli.duration,
        ),
    }
}

// Track which routes ever produced a sample and which ever dropped a stream-data
// packet. A route is only "missing metadata" if it dropped data but never parsed
// anything; routes that parse fine but drop a few leading/boundary packets are
// working as intended.
fn record_parse_result(
    parsed: &mut HashSet<DeviceRoute>,
    unparsed: &mut HashSet<DeviceRoute>,
    pkt: &tio::Packet,
    samples_len: usize,
) {
    if samples_len != 0 {
        parsed.insert(pkt.routing.clone());
        return;
    }
    if let tio::proto::Payload::StreamData(data) = &pkt.payload {
        if !data.data.is_empty() {
            unparsed.insert(pkt.routing.clone());
        }
    }
}

fn unparseable_routes(
    parsed: &HashSet<DeviceRoute>,
    unparsed: &HashSet<DeviceRoute>,
) -> Vec<DeviceRoute> {
    unparsed.difference(parsed).cloned().collect()
}

fn report_missing_metadata(mut routes: Vec<DeviceRoute>) {
    if routes.is_empty() {
        return;
    }
    routes.sort();
    crate::multi_progress().suspend(|| {
        use console::style;
        let warning = style("Warning").yellow().bold();
        let suggestion = style("Suggestion").yellow().bold();
        if routes.len() == 1 {
            eprintln!(
                "{}: stream data at route {} could not be parsed because metadata is missing or incompatible.",
                warning, routes[0]
            );
        } else {
            eprintln!(
                "{}: stream data at these routes could not be parsed because metadata is missing or incompatible:",
                warning
            );
            for route in routes.iter().take(5) {
                eprintln!("  {}", route);
            }
            if routes.len() > 5 {
                eprintln!("  ... and {} more", routes.len() - 5);
            }
        }
        eprintln!(
            "{}: ensure the log includes metadata or capture it with `tio log metadata`, including it as an argument before the log.",
            suggestion
        );
    });
}

fn ensure_open<'a>(fo: &'a mut Option<File>, path: &str) -> eyre::Result<&'a mut File> {
    use eyre::WrapErr;
    if fo.is_none() {
        *fo = Some(
            File::create(path).wrap_err_with(|| format!("could not create log file {}", path))?,
        );
    }
    Ok(fo.as_mut().unwrap())
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
        Some(d) => format!(
            "{{spinner}} [{{elapsed_precise}}/{}] {{decimal_bytes}} → {{msg}}",
            indicatif::FormattedDuration(d),
        ),
        None => "{spinner} [{elapsed_precise}] {decimal_bytes} → {msg}".to_string(),
    };
    let pb = crate::multi_progress().add(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::with_template(&template).unwrap());
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
            .wrap_err_with(|| format!("could not open port on {}", tio.root))
            .with_proxy_help()?;

        let mut file_out: Option<File> = None;

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
            let f = ensure_open(&mut file_out, &file)?;
            f.write_all(&serialized)
                .wrap_err_with(|| format!("failed to write {}", file))?;
            bytes_written += serialized.len() as u64;
            pb.set_position(bytes_written);
            pb.set_message(render_msg(samples_dropped));
            if unbuffered {
                f.flush()
                    .wrap_err_with(|| format!("failed to flush {}", file))?;
            }
        }
        pb.finish_and_clear();
        if duration_elapsed() {
            if bytes_written == 0 {
                log::info!("no data received");
            } else {
                log::info!("wrote {} bytes to {}", bytes_written, file);
            }
            return Ok(());
        } else if bytes_written == 0 {
            return Err(eyre::eyre!("stream ended; no data received"));
        } else {
            return Err(eyre::eyre!(
                "stream ended after writing {} bytes to {}",
                bytes_written,
                file
            ));
        }
    }

    let mut devs = DeviceTree::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open device tree on {}", tio.root))
        .with_proxy_help()?;

    let mut file_out: Option<File> = None;

    let write_packet = |pkt: tio::Packet, fo: &mut Option<File>, b: &mut u64| -> eyre::Result<()> {
        let serialized = pkt
            .serialize()
            .map_err(|_| eyre::eyre!("failed to serialize packet for log"))?;
        let f = ensure_open(fo, &file)?;
        f.write_all(&serialized)
            .wrap_err_with(|| format!("failed to write {}", file))?;
        *b += serialized.len() as u64;
        Ok(())
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
                            let count = received.wrapping_sub(expected);
                            samples_dropped += count as u64;
                            log::warn!(
                                "{}/{} dropped {} samples",
                                sample_route,
                                sample.stream.name,
                                count
                            );
                        }
                        write_packet(
                            sample.device.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        )?;
                        write_packet(
                            sample.stream.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        )?;
                        write_packet(
                            sample.segment.make_update_with_route(sample_route.clone()),
                            &mut file_out,
                            &mut bytes_written,
                        )?;
                        for col in &sample.columns {
                            write_packet(
                                col.desc.make_update_with_route(sample_route.clone()),
                                &mut file_out,
                                &mut bytes_written,
                            )?;
                        }
                    }

                    if sample.n == sample.source.first_sample_n {
                        let data_pkt = tio::Packet {
                            payload: tio::proto::Payload::StreamData(sample.source),
                            routing: sample_route,
                            ttl: 0,
                        };
                        write_packet(data_pkt, &mut file_out, &mut bytes_written)?;
                    }
                }
                pb.set_position(bytes_written);
                pb.set_message(render_msg(samples_dropped));
            }
            Err(e) => {
                pb.finish_and_clear();
                return Err(eyre::Report::new(e).wrap_err("stream ended"));
            }
        }

        if unbuffered {
            if let Some(f) = file_out.as_mut() {
                let _ = f.flush();
            }
        }
    }
    pb.finish_and_clear();
    if bytes_written == 0 {
        log::info!("no data received");
    } else {
        log::info!(
            "wrote {} bytes to {} ({} samples dropped)",
            bytes_written,
            file,
            samples_dropped
        );
    }
    Ok(())
}

pub fn log_metadata(tio: &TioOpts, file: String) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();

    let mut device = Device::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;

    let meta = device
        .get_metadata()
        .wrap_err("failed to fetch device metadata")?;

    let mut file_out: Option<File> = None;

    let write_packet = |fo: &mut Option<File>, pkt: tio::Packet| -> eyre::Result<()> {
        let raw = pkt
            .serialize()
            .map_err(|_| eyre::eyre!("failed to serialize metadata packet"))?;
        let f = ensure_open(fo, &file)?;
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

pub fn meta_reroute(input: String, route: DeviceRoute, output: Option<String>) -> eyre::Result<()> {
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

    let new_route = route;
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
    sensor: DeviceRoute,
    depth: Option<usize>,
) -> eyre::Result<()> {
    use eyre::WrapErr;

    let target_route = sensor;
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
            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(e) => {
                            log::warn!(
                                "{}: parse error at offset {} ({:?}); stopping",
                                path,
                                file_data.len() - rest.len(),
                                e
                            );
                            break;
                        }
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
            let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
            let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();

            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(e) => {
                            log::warn!(
                                "{}: parse error at offset {} ({:?}); stopping",
                                path,
                                file_data.len() - rest.len(),
                                e
                            );
                            break;
                        }
                    };
                    rest = &rest[len..];

                    let parser = parsers
                        .entry(pkt.routing.clone())
                        .or_insert_with(|| DeviceDataParser::new(ignore_session));

                    let samples = parser.process_packet(&pkt);
                    record_parse_result(
                        &mut parsed_routes,
                        &mut unparsed_routes,
                        &pkt,
                        samples.len(),
                    );

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

            let missing_routes: Vec<_> = unparseable_routes(&parsed_routes, &unparsed_routes)
                .into_iter()
                .filter(route_matches)
                .collect();
            report_missing_metadata(missing_routes);
        }
    }

    // Warn if nothing printed but data exists at deeper routes
    if !printed_any && !deeper_routes.is_empty() {
        let mut routes: Vec<_> = deeper_routes.into_iter().collect();
        routes.sort();
        eprintln!("No data at route {}, but found data at:", target_route);
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
    use twinleaf::data::{BoundaryReason, DeviceDataParser, StreamId};

    let file = File::open(path).wrap_err_with(|| format!("could not open {}", path))?;
    let mmap = unsafe { Mmap::map(&file) }.wrap_err_with(|| format!("could not mmap {}", path))?;
    let total_bytes = mmap.len() as u64;

    let pb = if total_bytes > 10 * 1024 * 1024 {
        let pb = crate::multi_progress().add(ProgressBar::new(total_bytes));
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

    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let mut streams: BTreeMap<(DeviceRoute, StreamId), StreamAgg> = BTreeMap::new();
    let mut devices: BTreeMap<DeviceRoute, DeviceAgg> = BTreeMap::new();
    let mut packet_count: u64 = 0;
    let mut session_changes: u64 = 0;
    let mut segment_changes: u64 = 0;

    let mut rest: &[u8] = &mmap[..];
    while !rest.is_empty() {
        let (pkt, len) = match tio::Packet::deserialize(rest) {
            Ok(r) => r,
            Err(e) => {
                log::warn!(
                    "{}: parse error at offset {} ({:?}); stopping",
                    path,
                    total_bytes - rest.len() as u64,
                    e
                );
                break;
            }
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
                            c.desc.data_type.type_name(),
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
    sensor: Option<DeviceRoute>,
    output: Option<String>,
    force: bool,
) -> eyre::Result<()> {
    use color_eyre::Help;
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};
    use memmap2::Mmap;

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

    let target =
        parse_csv_target(&stream_arg).map_err(|e| eyre::eyre!(e).suggestion(usage_hint))?;

    // A route may come from the selector prefix (e.g. /0/field) or -s, but not both.
    let target_route = match (target.route, sensor) {
        (Some(_), Some(_)) => {
            return Err(eyre::eyre!(
                "route given both in the selector and with -s; specify it only once"
            )
            .suggestion(usage_hint));
        }
        (Some(r), None) | (None, Some(r)) => r,
        (None, None) => DeviceRoute::root(),
    };

    // How the user referred to the stream, for error/summary messages.
    let target_desc = match &target.stream {
        StreamSel::Id(id) => id.to_string(),
        StreamSel::Name(name) => name.clone(),
    };

    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;
    let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();

    // The output path is built lazily on the first matching sample, once the
    // stream's canonical name is known (so an id selector still names the file
    // after the stream, and the route is part of the name to avoid collisions).
    let mut output_path: Option<String> = None;
    let mut resolved_name: Option<String> = None;

    let mut file: Option<File> = None;
    let mut header_written: bool = false;
    let mut header_cols: Vec<String> = Vec::new();
    let mut rows_written: u64 = 0;

    for path in &files {
        let f = File::open(path)
            .wrap_err_with(|| format!("could not read log file {}", path))
            .suggestion(usage_hint)?;
        let mmap = unsafe { Mmap::map(&f) }.wrap_err_with(|| format!("could not mmap {}", path))?;
        let total_bytes = mmap.len() as u64;

        let pb = crate::multi_progress().add(ProgressBar::new(total_bytes));
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
                Err(e) => {
                    log::warn!(
                        "{}: parse error at offset {} ({:?}); stopping",
                        path,
                        total_bytes - rest.len() as u64,
                        e
                    );
                    break;
                }
            };
            rest = &rest[len..];
            pb.set_position(total_bytes - rest.len() as u64);

            let parser = parsers
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceDataParser::new(ignore_session));
            let samples = parser.process_packet(&pkt);

            if pkt.routing == target_route {
                record_parse_result(
                    &mut parsed_routes,
                    &mut unparsed_routes,
                    &pkt,
                    samples.len(),
                );
            }

            if pkt.routing != target_route {
                continue;
            }

            for sample in samples {
                let is_match = match &target.stream {
                    StreamSel::Id(id) => sample.stream.stream_id == *id,
                    StreamSel::Name(name) => &sample.stream.name == name,
                };

                if !is_match {
                    continue;
                }

                if !header_written {
                    header_cols = vec!["time".to_string()];
                    header_cols.extend(sample.columns.iter().map(|col| col.desc.name.clone()));

                    if file.is_none() {
                        let prefix = output
                            .clone()
                            .unwrap_or_else(|| files.last().cloned().unwrap_or_default());
                        let route_label = route_filename_label(&target_route);
                        let path = format!("{}.{}.{}.csv", prefix, route_label, sample.stream.name);
                        if !force && std::path::Path::new(&path).exists() {
                            return Err(eyre::eyre!("output {} already exists", path).suggestion(
                                "pass --force to overwrite, or use -o for a different name",
                            ));
                        }
                        file = Some(
                            OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&path)
                                .wrap_err_with(|| format!("could not open {}", path))?,
                        );
                        resolved_name = Some(sample.stream.name.clone());
                        output_path = Some(path);
                    }
                    let path = output_path.as_deref().unwrap_or_default();
                    writeln!(file.as_mut().unwrap(), "{}", header_cols.join(","))
                        .wrap_err_with(|| format!("failed to write {}", path))?;
                    header_written = true;
                }

                let mut values: Vec<String> = Vec::new();
                values.push(format!("{:.6}", sample.timestamp_end()));

                values.extend(sample.columns.iter().map(|col| col.value.to_string()));

                let path = output_path.as_deref().unwrap_or_default();
                writeln!(file.as_mut().unwrap(), "{}", values.join(","))
                    .wrap_err_with(|| format!("failed to write {}", path))?;
                rows_written += 1;
            }
        }

        pb.finish_and_clear();
    }

    if !header_written {
        if unparsed_routes.contains(&target_route) && !parsed_routes.contains(&target_route) {
            return Err(eyre::eyre!(
                "stream data at route {} could not be parsed because metadata is missing or incompatible",
                target_route
            )
            .suggestion(
                "ensure the log includes metadata or capture it with `tio log metadata`, \
                 including it as an argument before the log",
            ));
        }
        return Err(eyre::eyre!(
            "no data found for stream '{}' at route {}",
            target_desc,
            target_route
        )
        .suggestion(format!(
            "see available routes and streams with: tio log dump -m {}",
            files.first().unwrap_or(&"<file>".to_string())
        )));
    }

    use console::style;
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:10}", s)).bold().cyan();
    let stream_label = resolved_name.as_deref().unwrap_or(&target_desc);

    println!();
    println!("{rule}");
    println!(" {}", style("CSV Output Summary").bold());
    println!("{rule}");
    println!(
        " {} {}",
        label("Output:"),
        output_path.as_deref().unwrap_or_default()
    );
    println!(" {} {} @ {}", label("Stream:"), stream_label, target_route);
    println!(" {} {}", label("Rows:"), rows_written);
    println!(" {} {}", label("Columns:"), header_cols.join(", "));
    println!("{rule}");

    Ok(())
}

/// Render a route as a filename-safe label: `root` for the device root, or the
/// hop indices joined by `.` (e.g. `/0/1` -> `0.1`).
fn route_filename_label(route: &DeviceRoute) -> String {
    if route.len() == 0 {
        "root".to_string()
    } else {
        route
            .iter()
            .map(|hop| hop.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }
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
    use eyre::WrapErr;
    use indicatif::{ProgressBar, ProgressStyle};
    use memmap2::Mmap;
    use std::path::Path;
    use twinleaf::data::{export, ColumnFilter, StreamKey};

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

    let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
    let ignore_session = files.len() > 1;
    let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut total_input_bytes: u64 = 0;

    println!("Processing {} files...", files.len());

    for path in &files {
        let file = File::open(&path).wrap_err_with(|| format!("could not open {}", path))?;
        let mmap =
            unsafe { Mmap::map(&file) }.wrap_err_with(|| format!("could not mmap {}", path))?;

        let total_bytes = mmap.len() as u64;
        total_input_bytes += total_bytes;
        let pb = crate::multi_progress().add(ProgressBar::new(total_bytes));
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
            record_parse_result(
                &mut parsed_routes,
                &mut unparsed_routes,
                &pkt,
                samples.len(),
            );

            for sample in samples {
                let key = StreamKey::new(pkt.routing.clone(), sample.stream.stream_id);

                if debug {
                    if let Some(ref boundary) = sample.boundary {
                        log::info!(
                            "[{}] sample_n={} boundary={:?}",
                            sample.stream.name,
                            sample.n,
                            boundary.reason
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

    report_missing_metadata(unparseable_routes(&parsed_routes, &unparsed_routes));

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

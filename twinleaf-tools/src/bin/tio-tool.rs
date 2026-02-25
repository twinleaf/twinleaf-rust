use clap::{Parser, Subcommand, ValueEnum};
use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::DeviceDataParser;
use twinleaf::device::{Device, DeviceTree, RpcClient};
use twinleaf::tio;
use twinleaf_tools::TioOpts;

use std::collections::HashMap;
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

/// Controls how runs are organized in the HDF5 output
#[derive(ValueEnum, Clone, Debug, Default)]
enum SplitLevel {
    /// No run splitting - flat structure: /{route}/{stream}/{datasets}
    #[default]
    None,
    /// Each stream has independent run counter
    Stream,
    /// All streams on a device share run counter
    Device,
    /// All streams globally share run counter
    Global,
}

#[cfg(feature = "hdf5")]
impl From<SplitLevel> for twinleaf::data::export::RunSplitLevel {
    fn from(level: SplitLevel) -> Self {
        match level {
            SplitLevel::None => Self::None,
            SplitLevel::Stream => Self::PerStream,
            SplitLevel::Device => Self::PerDevice,
            SplitLevel::Global => Self::Global,
        }
    }
}

/// Controls when discontinuities trigger run splits
#[derive(ValueEnum, Clone, Debug, Default)]
enum SplitPolicy {
    /// Split on any discontinuity (gaps, rate changes, etc.)
    #[default]
    Continuous,
    /// Only split when time goes backward (allows gaps)
    Monotonic,
}

#[cfg(feature = "hdf5")]
impl From<SplitPolicy> for twinleaf::data::export::SplitPolicy {
    fn from(policy: SplitPolicy) -> Self {
        match policy {
            SplitPolicy::Continuous => Self::Continuous,
            SplitPolicy::Monotonic => Self::Monotonic,
        }
    }
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

        /// Trigger a capture before dumping
        #[arg(long)]
        capture: bool,
    },

    /// Dump data from a live device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// Show parsed data samples
        #[arg(short = 'd', long = "data")]
        data: bool,

        /// Show metadata on boundaries
        #[arg(short = 'm', long = "meta")]
        meta: bool,

        /// Routing depth limit (default: unlimited)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Log samples to a file (includes metadata by default)
    Log {
        #[command(flatten)]
        tio: TioOpts,

        /// Output log file path
        #[arg(short = 'f', default_value_t = default_log_path())]
        file: String,

        /// Unbuffered output (flush every packet)
        #[arg(short = 'u')]
        unbuffered: bool,

        /// Raw mode: skip metadata request and dump all packets
        #[arg(long)]
        raw: bool,

        /// Routing depth (only used in --raw mode)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Log metadata to a file
    LogMetadata {
        #[command(flatten)]
        tio: TioOpts,

        /// Output metadata file path
        #[arg(short = 'f', default_value = "meta.tio")]
        file: String,
    },

    /// Dump data from binary log file(s)
    LogDump {
        /// Input log file(s)
        files: Vec<String>,

        /// Show parsed data samples
        #[arg(short = 'd', long = "data")]
        data: bool,

        /// Show metadata on boundaries
        #[arg(short = 'm', long = "meta")]
        meta: bool,

        /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
        #[arg(short = 's', long = "sensor", default_value = "/")]
        sensor: String,

        /// Routing depth limit (default: unlimited)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Dump parsed data from binary log file(s) [DEPRECATED: use log-dump -d]
    #[command(hide = true)]
    LogDataDump {
        /// Input log file(s)
        files: Vec<String>,
    },

    /// Convert binary log data to CSV
    LogCsv {
        /// Stream ID (e.g., 1) or Name (e.g., "vector", "field")
        stream: String,

        /// Input log file(s)
        files: Vec<String>,

        /// Sensor route in the device tree (default: /)
        #[arg(short = 's')]
        sensor: Option<String>,

        /// External metadata file path (optional)
        #[arg(short = 'm')]
        metadata: Option<String>,

        /// Output filename prefix
        #[arg(short = 'o')]
        output: Option<String>,
    },

    /// Convert binary log files to HDF5 format
    LogHdf {
        /// Input log file(s)
        files: Vec<String>,

        /// Output file path (defaults to input filename with .h5 extension)
        #[arg(short = 'o')]
        output: Option<String>,

        /// Filter streams using a glob pattern (e.g. "/*/vector")
        #[arg(short = 'g', long = "glob")]
        filter: Option<String>,

        /// Enable deflate compression (saves space, slows down write significantly)
        #[arg(short = 'c', long = "compress")]
        compress: bool,

        /// Enable debug output for glob matching
        #[arg(short = 'd', long)]
        debug: bool,

        /// How to organize runs in the output (none=flat, stream=per-stream, device=per-device, global=all-shared)
        #[arg(short = 'l', long = "split", default_value = "none")]
        split_level: SplitLevel,

        /// When to detect discontinuities (continuous=any gap, monotonic=only time backward)
        #[arg(short = 'p', long = "policy", default_value = "continuous")]
        split_policy: SplitPolicy,
    },

    /// Upgrade device firmware
    FirmwareUpgrade {
        #[command(flatten)]
        tio: TioOpts,

        /// Input firmware image path
        firmware_path: String,
    },

    /// Dump data samples from the device [DEPRECATED: use dump -d -s <ROUTE>]
    #[command(hide = true)]
    DataDump {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump data samples from all devices in the tree [DEPRECATED: use dump -a -d]
    #[command(hide = true)]
    DataDumpAll {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump device metadata [DEPRECATED: use dump -m -s <ROUTE>]
    #[command(hide = true)]
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
    let rpc_client = RpcClient::open(&proxy, route.clone()).expect("Failed to open RPC client");
    let rpcs = rpc_client.rpc_list(&route).map_err(|e| {
        eprintln!("RPC list failed: {:?}", e);
    })?;

    for (meta, name) in &rpcs.list {
        let spec = twinleaf::device::util::parse_rpc_spec(*meta, name.to_string());
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

fn dump(tio: &TioOpts, data: bool, meta: bool, depth: Option<usize>) -> Result<(), ()> {
    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.parse_route();

    // max_depth: None means unlimited (default), Some(n) limits to n levels
    let max_depth = depth;

    let route_matches = |sample_route: &DeviceRoute| -> bool {
        match route.relative_route(sample_route) {
            Ok(rel) => max_depth.map_or(true, |max| rel.len() <= max),
            Err(_) => false,
        }
    };

    // Raw mode (no flags): use raw port for packet dump
    if !data && !meta {
        let port_depth = max_depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);
        let port = proxy
            .new_port(None, route, port_depth, true, true)
            .map_err(|e| {
                eprintln!("Failed to initialize proxy port: {:?}", e);
            })?;

        for pkt in port.iter() {
            println!("{:?}", pkt);
        }
        return Ok(());
    }

    // Parsed mode (-d and/or -m): use DeviceTree for proper metadata handling
    let mut tree = DeviceTree::open(&proxy, route.clone()).map_err(|e| {
        eprintln!("Failed to open device tree: {:?}", e);
    })?;

    loop {
        match tree.next() {
            Ok((sample, sample_route)) => {
                if !route_matches(&sample_route) {
                    continue;
                }

                let route_opt = if max_depth.map_or(true, |d| d > 0) {
                    Some(&sample_route)
                } else {
                    None
                };
                print_sample(&sample, route_opt, meta, data);
            }
            Err(e) => {
                eprintln!("Device error: {:?}", e);
                break;
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

// Deprecated wrappers
fn data_dump_deprecated(tio: &TioOpts) -> Result<(), ()> {
    eprintln!("Warning: data-dump is deprecated, use 'dump -d -m --depth 0' instead");
    eprintln!();
    dump(tio, true, true, Some(0))
}

fn data_dump_all_deprecated(tio: &TioOpts) -> Result<(), ()> {
    eprintln!("Warning: data-dump-all is deprecated, use 'dump -d -m' instead");
    eprintln!();
    dump(tio, true, true, None)
}

fn meta_dump_deprecated(tio: &TioOpts) -> Result<(), ()> {
    eprintln!("Warning: meta-dump is deprecated, use 'dump -m --depth 0' instead");
    eprintln!();
    dump(tio, false, true, Some(0))
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

fn log_dump(
    files: Vec<String>,
    data: bool,
    meta: bool,
    sensor: String,
    depth: Option<usize>,
) -> Result<(), ()> {
    use std::collections::HashSet;

    if files.is_empty() {
        eprintln!("No input files specified");
        return Err(());
    }

    let target_route = DeviceRoute::from_str(&sensor).unwrap_or_else(|_| DeviceRoute::root());

    // max_depth: None means unlimited (default), Some(n) limits to n levels
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

    // Raw mode (no -d or -m): dump raw packets
    if !data && !meta {
        for path in files {
            let file_data =
                std::fs::read(&path).map_err(|e| eprintln!("Failed to read {}: {}", path, e))?;
            let mut rest: &[u8] = &file_data;
            while !rest.is_empty() {
                let (pkt, len) = tio::Packet::deserialize(rest).map_err(|_| {
                    eprintln!("Failed to parse packet");
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
    } else {
        // Parsed mode (-d and/or -m): stream samples with print_sample
        let mut parsers: HashMap<DeviceRoute, DeviceDataParser> = HashMap::new();
        let ignore_session = files.len() > 1;

        for path in files {
            let file_data =
                std::fs::read(&path).map_err(|e| eprintln!("Failed to read {}: {}", path, e))?;
            let mut rest: &[u8] = &file_data;
            while !rest.is_empty() {
                let (pkt, len) = match tio::Packet::deserialize(rest) {
                    Ok(res) => res,
                    Err(_) => break,
                };
                rest = &rest[len..];

                // Always process packet (for metadata building), but only print if route matches
                let parser = parsers
                    .entry(pkt.routing.clone())
                    .or_insert_with(|| DeviceDataParser::new(ignore_session));

                for sample in parser.process_packet(&pkt) {
                    if route_matches(&pkt.routing) {
                        print_sample(&sample, Some(&pkt.routing), meta, data);
                        printed_any = true;
                    } else if in_subtree(&pkt.routing) {
                        deeper_routes.insert(pkt.routing.clone());
                    }
                }
            }
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

fn log_data_dump_deprecated(files: Vec<String>) -> Result<(), ()> {
    eprintln!("Warning: log-data-dump is deprecated, use 'log-dump -d -m' instead");
    eprintln!();
    log_dump(files, true, true, "/".to_string(), None)
}

fn log_csv(
    stream_arg: String,
    files: Vec<String>,
    sensor: Option<String>,
    metadata: Option<String>,
    output: Option<String>,
) -> Result<(), ()> {
    if files.is_empty() {
        eprintln!("Invalid invocation: missing log file");
        return Err(());
    }

    let target_id = stream_arg.parse::<u8>().ok();

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

            let parser = parsers
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceDataParser::new(ignore_session));
            for _ in parser.process_packet(&pkt) {}
        }
    }

    let output_path = format!(
        "{}.{}.csv",
        output.unwrap_or_else(|| files[0].clone()),
        stream_arg
    );

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&output_path)
        .or(Err(()))?;

    let mut header_written: bool = false;

    for path in &files {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];

            let parser = parsers
                .entry(pkt.routing.clone())
                .or_insert_with(|| DeviceDataParser::new(ignore_session));
            let samples = parser.process_packet(&pkt);

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

    if !header_written {
        drop(file);
        std::fs::remove_file(&output_path).ok();
        eprintln!(
            "Error: No data found for stream '{}' at route {}",
            stream_arg, target_route
        );
        eprintln!();
        eprintln!("To see available routes and streams, run:");
        eprintln!(
            "  tio-tool log-dump -m {}",
            files.first().unwrap_or(&"<file>".to_string())
        );
        return Err(());
    }

    Ok(())
}

#[cfg(feature = "hdf5")]
fn log_hdf(
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
            let input_path = Path::new(&files[0]);
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

            for sample in parser.process_packet(&pkt) {
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
fn log_hdf(
    _files: Vec<String>,
    _output: Option<String>,
    _filter: Option<String>,
    _compress: bool,
    _debug: bool,
    _split_level: SplitLevel,
    _split_policy: SplitPolicy,
) -> Result<(), ()> {
    eprintln!("Error: This version of tio-tool was compiled without HDF5 support.");
    eprintln!("To enable it, reinstall with:");
    eprintln!("  cargo install twinleaf-tools --features hdf5");
    Err(())
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
        Commands::Dump {
            tio,
            data,
            meta,
            depth,
        } => dump(&tio, data, meta, depth),
        Commands::Log {
            tio,
            file,
            unbuffered,
            raw,
            depth,
        } => log(&tio, file, unbuffered, raw, depth),
        Commands::LogMetadata { tio, file } => log_metadata(&tio, file),
        Commands::LogDump {
            files,
            data,
            meta,
            sensor,
            depth,
        } => log_dump(files, data, meta, sensor, depth),
        Commands::LogDataDump { files } => log_data_dump_deprecated(files),
        Commands::LogCsv {
            stream,
            files,
            sensor,
            metadata,
            output,
        } => log_csv(stream, files, sensor, metadata, output),
        Commands::LogHdf {
            files,
            output,
            filter,
            compress,
            debug,
            split_level,
            split_policy,
        } => log_hdf(
            files,
            output,
            filter,
            compress,
            debug,
            split_level,
            split_policy,
        ),
        Commands::FirmwareUpgrade { tio, firmware_path } => firmware_upgrade(&tio, firmware_path),
        Commands::DataDump { tio } => data_dump_deprecated(&tio),
        Commands::DataDumpAll { tio } => data_dump_all_deprecated(&tio),
        Commands::MetaDump { tio } => meta_dump_deprecated(&tio),
    };

    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        eprintln!("FAILED");
        ExitCode::FAILURE
    }
}

use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::data::{ColumnData, DeviceDataParser};
use twinleaf::tio;

use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;

use getopts::Options;

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

fn tio_opts() -> Options {
    let mut opts = Options::new();
    opts.optopt(
        "r",
        "",
        &format!("sensor root (default {})", util::default_proxy_url()),
        "address",
    );
    opts.optopt(
        "s",
        "",
        "sensor path in the sensor tree (default /)",
        "path",
    );
    opts
}

fn tio_parseopts(opts: Options, args: &[String]) -> (getopts::Matches, String, DeviceRoute) {
    let matches = match opts.parse(args) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    let root = if let Some(url) = matches.opt_str("r") {
        url
    } else {
        "tcp://localhost".to_string()
    };
    let route = if let Some(path) = matches.opt_str("s") {
        DeviceRoute::from_str(&path).unwrap()
    } else {
        DeviceRoute::root()
    };
    (matches, root, route)
}

fn list_rpcs(args: &[String]) -> std::io::Result<()> {
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, args);

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

fn get_rpctype(
    name: &String,
    device: &proxy::Port,
    //    tx: &crossbeam::channel::Sender<proto::Packet>,
    //    rx: &crossbeam::channel::Receiver<proto::Packet>,
) -> String {
    //tx.send(util::Rpc::make_request("rpc.info", name.as_bytes()))
    //    .unwrap();
    //RpcMeta::parse(u16::from_le_bytes(
    //    rpc_get_reply(&rx, 2).unwrap()[0..2].try_into().unwrap(),
    //))
    RpcMeta::parse(device.rpc("rpc.info", name).unwrap()).arg_type
}

fn rpc(args: &[String]) -> std::io::Result<()> {
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
    let (matches, root, route) = tio_parseopts(opts, args);

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
            Some(if t == "" { "string".to_string() } else { t })
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
            if debug {
                drop(proxy);
                println!("RPC failed: {:?}", err);
                for s in proxy_status.try_iter() {
                    println!("{:?}", s);
                }
            }
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "RPC failed"));
        }
    };

    if reply.len() != 0 {
        let rep_type = if let Some(rep_type) = matches.opt_str("rep-type") {
            Some(rep_type)
        } else {
            if let None = req_type {
                let t = get_rpctype(&rpc_name, &device);
                Some(if t == "" { "string".to_string() } else { t })
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

fn rpc_dump(args: &[String]) -> std::io::Result<()> {
    let opts = tio_opts();
    let (matches, root, route) = tio_parseopts(opts, args);

    let rpc_name = if matches.free.len() != 1 {
        panic!("must specify rpc name")
    } else {
        matches.free[0].clone()
    };

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_rpc(route).unwrap();

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
        std::io::stdout().write(&full_reply)?;
    }
    Ok(())
}

fn dump(args: &[String]) {
    let opts = tio_opts();
    let (_matches, root, _route) = tio_parseopts(opts, args);

    let proxy = proxy::Interface::new(&root);

    for pkt in proxy.tree_full().unwrap().iter() {
        println!("{:?}", pkt);
    }
}

fn meta_dump(args: &[String]) {
    use twinleaf::data::Device;
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_full(route).unwrap();
    let mut device = Device::new(device);

    let meta = device.get_metadata();
    println!("{:?}", meta.device);
    for (_id, stream) in meta.streams {
        println!("{:?}", stream.stream);
        println!("{:?}", stream.segment);
        for col in stream.columns {
            println!("{:?}", col);
        }
    }
}

fn print_sample(sample: &twinleaf::data::Sample) {
    use twinleaf::data::ColumnData;
    if sample.meta_changed {
        println!("# DEVICE {:?}", sample.device);
        println!("# STREAM {:?}", sample.stream);
        for col in &sample.columns {
            println!("# COLUMN {:?}", col.desc);
        }
    }
    if sample.segment_changed {
        println!("# SEGMENT {:?}", sample.segment);
    }
    print!(
        "SAMPLE({}:{}) {:.6}",
        sample.stream.stream_id,
        sample.segment.segment_id,
        sample.timestamp_end()
    );
    for col in &sample.columns {
        print!(
            " {}: {}",
            col.desc.name,
            match col.value {
                ColumnData::Int(x) => format!("{}", x),
                ColumnData::UInt(x) => format!("{}", x),
                ColumnData::Float(x) => format!("{}", x),
                ColumnData::Unknown => "?".to_string(),
            }
        );
    }
    println!(" [#{}]", sample.n);
}

fn data_dump(args: &[String]) {
    use twinleaf::data::Device;
    let opts = tio_opts();
    let (_matches, root, route) = tio_parseopts(opts, args);

    let proxy = proxy::Interface::new(&root);
    let device = proxy.device_full(route).unwrap();
    let mut device = Device::new(device);

    loop {
        print_sample(&device.next());
    }
}

fn log(args: &[String]) {
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
    let (matches, root, route) = tio_parseopts(opts, args);

    let output_path = if let Some(path) = matches.opt_str("f") {
        path
    } else {
        output_path.to_string()
    };

    let proxy = proxy::Interface::new(&root);

    let mut file = File::create(output_path).unwrap();
    let sync = matches.opt_present("u");

    for pkt in proxy.device_full(route).unwrap().iter() {
        let raw = pkt.serialize().unwrap();
        file.write_all(&raw).unwrap();
        if sync {
            file.flush().unwrap();
        }
    }
}

fn log_dump(args: &[String]) {
    for path in args {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            println!("{:?}", pkt);
        }
    }
}

fn log_data_dump(args: &[String]) {
    use twinleaf::data::DeviceDataParser;
    let mut parser = DeviceDataParser::new(args.len() > 1);

    for path in args {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            for sample in parser.process_packet(&pkt) {
                print_sample(&sample);
            }
        }
    }
}

//match
fn match_value(data: ColumnData) -> String {
    let data_type = match data {
        ColumnData::Int(x) => format!("{}", x),
        ColumnData::UInt(x) => format!("{}", x),
        ColumnData::Float(x) => format!("{}", x),
        ColumnData::Unknown => "?".to_string(),
    };
    data_type
}

fn log_csv(args: &[String]) -> std::io::Result<()> {
    let mut parser = DeviceDataParser::new(args.len() > 1);
    let id: u8 = args[1].parse().unwrap();
    let output_name = args.get(3).unwrap_or(&args[2]);

    let s = output_name.replace("csv", "");
    let path = format!("{}{}.csv", s, &args[1]).to_string();

    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    let mut streamhead: bool = false;
    let mut first: bool = true;

    for path in &args[2..] {
        let mut rest: &[u8] = &std::fs::read(path).unwrap();
        while rest.len() > 0 {
            let (pkt, len) = tio::Packet::deserialize(rest).unwrap();
            rest = &rest[len..];
            for sample in parser.process_packet(&pkt) {
                //match stream id
                if sample.stream.stream_id == id as u8 {
                    //iterate through values
                    for col in &sample.columns {
                        let time = format!("{:.6}", sample.timestamp_end());
                        let value = match_value(col.value.clone());

                        //write in column names
                        if !streamhead {
                            let timehead = format!("{},", "time");
                            let _ = file.write_all(timehead.as_bytes());

                            for col in &sample.columns {
                                let mut header = format!("{},", col.desc.name);

                                if col.desc.name
                                    == sample.columns[&sample.columns.len() - 1].desc.name.clone()
                                {
                                    header = format!("{}", col.desc.name);
                                }

                                file.write_all(header.as_bytes())?;
                            }
                            file.write_all(b"\n")?;
                            streamhead = true;
                        }

                        //write in data
                        let timefmt = format!("{},", time);
                        let mut formatted_value = format!("{},", value);
                        if first {
                            let _ = file.write_all(timefmt.as_bytes());
                            first = false;
                        }

                        if value
                            == match_value(sample.columns[&sample.columns.len() - 1].value.clone())
                        {
                            formatted_value = format!("{}", value);
                        }

                        file.write_all(formatted_value.as_bytes())?;
                    }
                    file.write_all(b"\n")?;
                    first = true;
                }
            }
        }
    }
    Ok(())
}

fn firmware_upgrade(args: &[String]) {
    let opts = tio_opts();
    let (matches, root, route) = tio_parseopts(opts, args);

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

    if let Err(_) = device.action("dev.firmware.upgrade") {
        panic!("upgrade failed");
    }
}

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("help".to_string());
    }
    match args[1].as_str() {
        "rpc-list" => {
            list_rpcs(&args[2..]).unwrap();
        }
        "rpc" => {
            rpc(&args[2..]).unwrap();
        }
        "rpc-dump" => {
            rpc_dump(&args[2..]).unwrap();
        }
        "dump" => {
            dump(&args[2..]); //.unwrap();
        }
        "log" => {
            log(&args[2..]); //.unwrap();
        }
        "log-dump" => {
            log_dump(&args[2..]); //.unwrap();
        }
        "log-data-dump" => {
            log_data_dump(&args[2..]); //.unwrap();
        }
        "log-csv" => {
            let _ = log_csv(&args[1..]); //.unwrap();
        }
        "firmware-upgrade" => {
            firmware_upgrade(&args[2..]); //.unwrap();
        }
        "data-dump" => {
            data_dump(&args[2..]); //.unwrap();
        }
        "meta-dump" => {
            meta_dump(&args[2..]); //.unwrap();
        }
        _ => {
            // TODO: do usage right
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool dump [-r url] [-s sensor]");
            println!(" tio-tool log [-r url] [-s sensor] [filename]");
            println!(" tio-tool log-dump filename [filename ...]");
            println!(" tio-tool log-data-dump filename [filename ...]");
            println!(" tio-tool log-csv <stream id> [metadata] <csv>");
            println!(" tio-tool rpc-list [-r url] [-s sensor]");
            println!(" tio-tool rpc [-r url] [-s sensor] [-t type] [-d] <rpc-name> [rpc-arg]");
            println!(" tio-tool rpc-dump [-r url] [-s sensor] <rpc-name>");
            println!(" tio-tool firmware-upgrade [-r url] [-s sensor] <firmware_image.bin>");
            println!(" tio-tool data-dump [-r url] [-s sensor]");
            println!(" tio-tool meta-dump [-r url] [-s sensor]");
        }
    }
}

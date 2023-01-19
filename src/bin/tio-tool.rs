use tio::proto::DeviceRoute;
use tio::proxy;
use tio::util;
use twinleaf::tio;

use std::env;

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

    let proxy = proxy::Port::new(&root, None, None);
    let device = util::DeviceRpc::new(&proxy, Some(route));

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
    device: &util::DeviceRpc,
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
        "",
        "RPC type (one of u8/u16/u32/u64 i8/i16/i32/i64 f32/f64 string). ",
        "type",
    );
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

    let proxy = proxy::Port::new(&root, None, None);
    let device = util::DeviceRpc::new(&proxy, Some(route));

    let mut rpc_type = if let Some(rpc_type) = matches.opt_str("t") {
        Some(rpc_type)
    } else {
        if rpc_arg.is_some() {
            let t = get_rpctype(&rpc_name, &device);
            Some(if t == "" { "string".to_string() } else { t })
        } else {
            None
        }
    };

    let reply = device
        .raw_rpc(
            &rpc_name,
            &if rpc_arg.is_none() {
                vec![]
            } else {
                let s = rpc_arg.unwrap();
                match &rpc_type.as_ref().unwrap()[..] {
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
        )
        .unwrap();

    if reply.len() != 0 {
        if rpc_type.is_none() {
            let t = get_rpctype(&rpc_name, &device);
            rpc_type = Some(if t == "" { "string".to_string() } else { t })
        }
        let reply_str = match &rpc_type.as_ref().unwrap()[..] {
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
    Ok(())
}

fn dump(args: &[String]) {
    let opts = tio_opts();
    let (_matches, root, _route) = tio_parseopts(opts, args);

    let proxy = proxy::Port::new(&root, None, None);
    let (_tx, rx) = proxy.full_port().unwrap();

    for pkt in rx.iter() {
        println!("{:?}", pkt);
    }
}

fn log(args: &[String]) {
    let opts = tio_opts();
    let (_matches, root, _route) = tio_parseopts(opts, args);

    let proxy = proxy::Port::new(&root, None, None);
    let (_tx, rx) = proxy.full_port().unwrap();

    let mut file = File::create("log.tio")?; // TODO: Nice filename with date?
    
    for pkt in rx.iter() {
        file.write_all(pkt)?;
    }

}

fn firmware_upgrade(args: &[String]) {
    let opts = tio_opts();
    let (matches, root, route) = tio_parseopts(opts, args);

    if matches.free.len() != 1 {
        panic!("Must specify firmware path only")
    }

    let firmware_data = std::fs::read(matches.free[0].clone()).unwrap();

    println!("Loaded {} bytes firmware", firmware_data.len());

    let proxy = proxy::Port::new(&root, None, None);
    let device = util::DeviceRpc::new(&proxy, Some(route));

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
        "dump" => {
            dump(&args[2..]); //.unwrap();
        }
        "log" => {
            log(&args[2..]); //.unwrap();
        }
        "firmware-upgrade" => {
            firmware_upgrade(&args[2..]); //.unwrap();
        }
        _ => {
            // TODO: do usage right
            println!("Usage:");
            println!(" tio-tool help");
            println!(" tio-tool dump [-r url] [-s sensor]");
            println!(" tio-tool rpc-list [-r url] [-s sensor]");
            println!(" tio-tool rpc [-r url] [-s sensor] [-t type] <rpc-name> [rpc-arg]");
            println!(" tio-tool firmware-upgrade [-r url] [-s sensor] <firmware_image.bin>");
        }
    }
}

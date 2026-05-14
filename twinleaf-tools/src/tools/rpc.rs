use std::io::Write;

use crate::{ProxyHelp, RPCSubcommands, RpcCli, TioOpts};
use tio::proxy;
use twinleaf::device::util::{rpc_decode_reply, rpc_encode_arg};
use twinleaf::device::{RpcClient, RpcRegistry, RpcValue, RpcValueType};
use twinleaf::tio;

pub fn run_rpc(rpc_cli: RpcCli) -> eyre::Result<()> {
    match rpc_cli.subcommands {
        Some(RPCSubcommands::List { tio }) => list_rpcs(&tio),
        Some(RPCSubcommands::Dump {
            tio,
            rpc_name,
            capture,
        }) => rpc_dump(&tio, rpc_name, capture),
        None => rpc(
            &rpc_cli.tio,
            rpc_cli.rpc_name.unwrap_or("".to_string()),
            rpc_cli.rpc_arg,
            rpc_cli.req_type,
            rpc_cli.rep_type,
            rpc_cli.debug,
        ),
    }
}

pub fn list_rpcs(tio: &TioOpts) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let rpc_client = RpcClient::open(&proxy, route.clone())
        .wrap_err_with(|| format!("could not open RPC client for {}", tio.root))
        .with_proxy_help()?;
    let rpcs = rpc_client
        .rpc_list(&route)
        .wrap_err("failed to query RPC list")?;
    let registry = RpcRegistry::from(&rpcs);

    for desc in registry.iter() {
        println!(
            "{} {}({})",
            desc.perm_str(),
            desc.full_name,
            desc.type_str()
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
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;

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
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;

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
